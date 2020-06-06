/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

/**
 * Codec for {@link LocalTime}.
 */
final class LocalTimeCodec extends AbstractClassedCodec<LocalTime> {

    private static final int HOURS_OF_DAY = 24;

    private static final int SECONDS_OF_DAY = HOURS_OF_DAY * 60 * 60;

    private static final long NANO_OF_DAY = ((long) SECONDS_OF_DAY) * 1000_000_000L;

    LocalTimeCodec(ByteBufAllocator allocator) {
        super(allocator, LocalTime.class);
    }

    @Override
    public LocalTime decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        if (binary) {
            return decodeBinary(value);
        } else {
            return readTimeText(value);
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalTime;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new LocalTimeParameter(allocator, (LocalTime) value);
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataTypes.TIME == info.getType();
    }

    static LocalTime readTimeText(ByteBuf buf) {
        boolean isNegative = readNegative(buf);
        int hour = DateTimes.readIntInDigits(buf);
        int minute = DateTimes.readIntInDigits(buf);
        int second = DateTimes.readIntInDigits(buf);

        if (isNegative) {
            // The `hour` is a positive integer.
            long totalSeconds = -(TimeUnit.HOURS.toSeconds(hour) + TimeUnit.MINUTES.toSeconds(minute) + second);
            return LocalTime.ofSecondOfDay(((totalSeconds % SECONDS_OF_DAY) + SECONDS_OF_DAY) % SECONDS_OF_DAY);
        } else {
            return LocalTime.of(hour % HOURS_OF_DAY, minute, second);
        }
    }

    static boolean readNegative(ByteBuf buf) {
        if (buf.getByte(buf.readerIndex()) == '-') {
            buf.skipBytes(1);
            return true;
        } else {
            return false;
        }
    }

    private static LocalTime decodeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < DateTimes.TIME_SIZE) {
            return LocalTime.MIDNIGHT;
        }

        boolean isNegative = buf.readBoolean();

        // Skip day part.
        buf.skipBytes(Integer.BYTES);

        short hour = buf.readUnsignedByte();
        short minute = buf.readUnsignedByte();
        short second = buf.readUnsignedByte();

        if (bytes < DateTimes.MICRO_TIME_SIZE) {
            if (isNegative) {
                // The `hour` is a positive integer.
                long totalSeconds = -(TimeUnit.HOURS.toSeconds(hour) + TimeUnit.MINUTES.toSeconds(minute) + second);
                return LocalTime.ofSecondOfDay(((totalSeconds % SECONDS_OF_DAY) + SECONDS_OF_DAY) % SECONDS_OF_DAY);
            } else {
                return LocalTime.of(hour % HOURS_OF_DAY, minute, second);
            }
        }

        long micros = buf.readUnsignedIntLE();

        if (isNegative) {
            long nanos = -(TimeUnit.HOURS.toNanos(hour) +
                TimeUnit.MINUTES.toNanos(minute) +
                TimeUnit.SECONDS.toNanos(second) +
                TimeUnit.MICROSECONDS.toNanos(micros));

            return LocalTime.ofNanoOfDay(((nanos % NANO_OF_DAY) + NANO_OF_DAY) % NANO_OF_DAY);
        } else {
            return LocalTime.of(hour % HOURS_OF_DAY, minute, second, (int) TimeUnit.MICROSECONDS.toNanos(micros));
        }
    }

    static void encodeTime(ParameterWriter writer, LocalTime time) {
        int micros = (int) TimeUnit.NANOSECONDS.toMicros(time.getNano());
        DurationCodec.encodeTime(writer, false, time.getHour(), time.getMinute(), time.getSecond(), micros);
    }

    private static final class LocalTimeParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final LocalTime time;

        private LocalTimeParameter(ByteBufAllocator allocator, LocalTime time) {
            this.allocator = allocator;
            this.time = time;
        }

        @Override
        public Mono<ByteBuf> binary() {
            return Mono.fromSupplier(() -> {
                if (LocalTime.MIDNIGHT.equals(time)) {
                    // It is zero of var int, not terminal.
                    return allocator.buffer(Byte.BYTES).writeByte(0);
                }

                int nanos = time.getNano();
                int size = nanos > 0 ? DateTimes.MICRO_TIME_SIZE : DateTimes.TIME_SIZE;

                ByteBuf buf = allocator.buffer(Byte.BYTES + size);

                try {
                    buf.writeByte(size)
                        .writeBoolean(false)
                        .writeIntLE(0)
                        .writeByte(time.getHour())
                        .writeByte(time.getMinute())
                        .writeByte(time.getSecond());

                    if (nanos > 0) {
                        return buf.writeIntLE(nanos / DateTimes.NANOS_OF_MICRO);
                    }

                    return buf;
                } catch (Throwable e) {
                    buf.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
            return Mono.fromRunnable(() -> encodeTime(writer, time));
        }

        @Override
        public short getType() {
            return DataTypes.TIME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LocalTimeParameter)) {
                return false;
            }

            LocalTimeParameter that = (LocalTimeParameter) o;

            return time.equals(that.time);
        }

        @Override
        public int hashCode() {
            return time.hashCode();
        }
    }
}
