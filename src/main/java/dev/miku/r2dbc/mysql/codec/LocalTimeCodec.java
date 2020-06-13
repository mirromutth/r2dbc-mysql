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

    private static final long NANOS_OF_DAY = ((long) SECONDS_OF_DAY) * NANOS_OF_SECOND;

    private static final long NANOS_OF_HOUR = ((long) SECONDS_OF_HOUR) * NANOS_OF_SECOND;

    private static final long NANOS_OF_MINUTE = ((long) SECONDS_OF_MINUTE) * NANOS_OF_SECOND;

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
                return negativeCircle(hour, minute, second, nano);
            } else {
                return LocalTime.of(hour % HOURS_OF_DAY, minute, second, nano);
            }
        }

        if (isNegative) {
            return negativeCircle(hour, minute, second);
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
                return negativeCircle(hour, minute, second);
            } else {
                return LocalTime.of(hour % HOURS_OF_DAY, minute, second);
            }
        }

        long nano = buf.readUnsignedIntLE() * NANOS_OF_MICRO;

        if (isNegative) {
            return negativeCircle(hour, minute, second, nano);
        } else {
            return LocalTime.of(hour % HOURS_OF_DAY, minute, second, (int) nano);
        }
    }

    static void encodeTime(ParameterWriter writer, LocalTime time) {
        int micros = time.getNano() / NANOS_OF_MICRO;
        DurationCodec.encodeTime(writer, false, time.getHour(), time.getMinute(), time.getSecond(), micros);
    }

    private static LocalTime negativeCircle(int hour, int minute, int second) {
        int total = -(hour * SECONDS_OF_HOUR + minute * SECONDS_OF_MINUTE + second);
        return LocalTime.ofSecondOfDay(((total % SECONDS_OF_DAY) + SECONDS_OF_DAY) % SECONDS_OF_DAY);
    }

    private static LocalTime negativeCircle(long hour, long minute, long second, long nano) {
        long total = -(hour * NANOS_OF_HOUR + minute * NANOS_OF_MINUTE +
            second * NANOS_OF_SECOND + nano);

        return LocalTime.ofNanoOfDay(((total % NANOS_OF_DAY) + NANOS_OF_DAY) % NANOS_OF_DAY);
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
