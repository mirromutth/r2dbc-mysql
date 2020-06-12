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

import static dev.miku.r2dbc.mysql.codec.DateTimes.*;

/**
 * Codec for {@link LocalTime}.
 */
final class LocalTimeCodec extends AbstractClassedCodec<LocalTime> {

    LocalTimeCodec(ByteBufAllocator allocator) {
        super(allocator, LocalTime.class);
    }

    @Override
    public LocalTime decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        return binary ? decodeBinary(value) : readTimeText(value);
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
        int hour = readIntInDigits(buf);
        int minute = readIntInDigits(buf);
        int second = readIntInDigits(buf);

        if (buf.isReadable()) {
            int nano = readMicroInDigits(buf) * NANOS_OF_MICRO;
            if (isNegative) {
                long totalNanos = -(((long) hour) * NANOS_OF_HOUR + ((long) minute) * NANOS_OF_MINUTE +
                    ((long) second) * NANOS_OF_SECOND + ((long) nano));
                return LocalTime.ofNanoOfDay(((totalNanos % NANOS_OF_DAY) + NANOS_OF_DAY) % NANOS_OF_DAY);
            } else {
                return LocalTime.of(hour % HOURS_OF_DAY, minute, second, nano);
            }
        }

        if (isNegative) {
            // The `hour` is a positive integer.
            long totalSeconds = -(((long) hour) * SECONDS_OF_HOUR + ((long) minute) * SECONDS_OF_MINUTE +
                ((long) second));
            return LocalTime.ofSecondOfDay(((totalSeconds % SECONDS_OF_DAY) + SECONDS_OF_DAY) % SECONDS_OF_DAY);
        } else {
            return LocalTime.of(hour % HOURS_OF_DAY, minute, second);
        }
    }

    static boolean readNegative(ByteBuf buf) {
        switch (buf.getByte(buf.readerIndex())) {
            case '-':
                buf.skipBytes(1);
                return true;
            case '+':
                buf.skipBytes(1);
                return false;
            default:
                return false;
        }
    }

    private static LocalTime decodeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < TIME_SIZE) {
            return LocalTime.MIDNIGHT;
        }

        boolean isNegative = buf.readBoolean();

        // Skip day part.
        buf.skipBytes(Integer.BYTES);

        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();

        if (bytes < MICRO_TIME_SIZE) {
            if (isNegative) {
                // The `hour` is a positive integer.
                int totalSeconds = -(((int) hour) * SECONDS_OF_HOUR + ((int) minute) * SECONDS_OF_MINUTE + ((int) second));
                return LocalTime.ofSecondOfDay(((totalSeconds % SECONDS_OF_DAY) + SECONDS_OF_DAY) % SECONDS_OF_DAY);
            } else {
                return LocalTime.of(hour % HOURS_OF_DAY, minute, second);
            }
        }

        long micros = buf.readUnsignedIntLE();

        if (isNegative) {
            long nanos = -(((long) hour) * NANOS_OF_HOUR + ((long) minute) * NANOS_OF_MINUTE +
                ((long) second) * NANOS_OF_SECOND + micros * NANOS_OF_MICRO);

            return LocalTime.ofNanoOfDay(((nanos % NANOS_OF_DAY) + NANOS_OF_DAY) % NANOS_OF_DAY);
        } else {
            return LocalTime.of(hour % HOURS_OF_DAY, minute, second, (int) (micros * NANOS_OF_MICRO));
        }
    }

    static void encodeTime(ParameterWriter writer, LocalTime time) {
        int micros = time.getNano() / NANOS_OF_MICRO;
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
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> {
                if (LocalTime.MIDNIGHT.equals(time)) {
                    // It is zero of var int, not terminal.
                    return allocator.buffer(Byte.BYTES).writeByte(0);
                }

                int nanos = time.getNano();
                int size = nanos > 0 ? MICRO_TIME_SIZE : TIME_SIZE;

                ByteBuf buf = allocator.buffer(Byte.BYTES + size);

                try {
                    buf.writeByte(size)
                        .writeBoolean(false)
                        .writeIntLE(0)
                        .writeByte(time.getHour())
                        .writeByte(time.getMinute())
                        .writeByte(time.getSecond());

                    if (nanos > 0) {
                        return buf.writeIntLE(nanos / NANOS_OF_MICRO);
                    }

                    return buf;
                } catch (Throwable e) {
                    buf.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
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
