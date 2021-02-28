/*
 * Copyright 2018-2021 the original author or authors.
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

import dev.miku.r2dbc.mysql.MySqlColumnMetadata;
import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static dev.miku.r2dbc.mysql.codec.DateTimes.MICRO_TIME_SIZE;
import static dev.miku.r2dbc.mysql.codec.DateTimes.NANOS_OF_MICRO;
import static dev.miku.r2dbc.mysql.codec.DateTimes.NANOS_OF_SECOND;
import static dev.miku.r2dbc.mysql.codec.DateTimes.SECONDS_OF_DAY;
import static dev.miku.r2dbc.mysql.codec.DateTimes.SECONDS_OF_HOUR;
import static dev.miku.r2dbc.mysql.codec.DateTimes.SECONDS_OF_MINUTE;
import static dev.miku.r2dbc.mysql.codec.DateTimes.TIME_SIZE;
import static dev.miku.r2dbc.mysql.codec.DateTimes.readIntInDigits;
import static dev.miku.r2dbc.mysql.codec.DateTimes.readMicroInDigits;

/**
 * Codec for {@link Duration}.
 */
final class DurationCodec extends AbstractClassedCodec<Duration> {

    DurationCodec(ByteBufAllocator allocator) {
        super(allocator, Duration.class);
    }

    @Override
    public Duration decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        return binary ? decodeBinary(value) : decodeText(value);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Duration;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new DurationParameter(allocator, (Duration) value);
    }

    @Override
    protected boolean doCanDecode(MySqlColumnMetadata metadata) {
        return metadata.getType() == MySqlType.TIME;
    }

    static void encodeTime(ParameterWriter writer, boolean isNegative, int hours, int minutes, int seconds,
        int micros) {
        if (isNegative) {
            writer.append('-');
        } else {
            // Time start with number, prepare writer to string mode.
            writer.startString();
        }

        if (hours < 10) {
            writer.append('0');
        }

        writer.writeInt(hours);
        writer.append(':');

        if (minutes < 10) {
            writer.append('0');
        }

        writer.writeInt(minutes);
        writer.append(':');

        if (seconds < 10) {
            writer.append('0');
        }

        writer.writeInt(seconds);

        // Must be greater than 0, can NOT use "micros != 0" here.
        // Sure, micros will never less than 0, but need to check for avoid inf loop.
        if (micros > 0) {
            writer.append('.');

            // WATCH OUT for inf loop: i from 100000 to 1, micros is greater than 0, 0 < micros < 1 is
            // impossible, so micros < 1 will be false finally, then loop done.Safe.
            for (int i = 100000; micros < i; i /= 10) {
                writer.append('0');
            }

            int m = micros;

            // WATCH OUT for inf loop: micros is greater than 0, that means it least contains one digit
            // which is not 0, so micros % 10 == 0 will be false finally, then loop done. Safe.
            while (m % 10 == 0) {
                m /= 10;
            }

            writer.writeInt(m);
        }
    }

    private static Duration decodeText(ByteBuf buf) {
        boolean isNegative = LocalTimeCodec.readNegative(buf);
        int hour = readIntInDigits(buf);
        int minute = readIntInDigits(buf);
        int second = readIntInDigits(buf);
        int totalSeconds = hour * SECONDS_OF_HOUR + minute * SECONDS_OF_MINUTE + second;

        if (buf.isReadable()) {
            int nano = readMicroInDigits(buf) * NANOS_OF_MICRO;
            return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds, isNegative ? -nano : nano);
        }

        return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds);
    }

    private static Duration decodeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < TIME_SIZE) {
            return Duration.ZERO;
        }

        boolean isNegative = buf.readBoolean();

        long day = buf.readUnsignedIntLE();
        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();
        long totalSeconds = day * SECONDS_OF_DAY + ((long) hour) * SECONDS_OF_HOUR +
            ((long) minute) * SECONDS_OF_MINUTE + second;

        if (bytes < MICRO_TIME_SIZE) {
            return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds);
        }

        long nanos = buf.readUnsignedIntLE() * NANOS_OF_MICRO;

        return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds, isNegative ? -nanos : nanos);
    }

    private static final class DurationParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final Duration value;

        private DurationParameter(ByteBufAllocator allocator, Duration value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> {
                long seconds = value.getSeconds();
                int nanos = value.getNano();

                if (seconds == 0 && nanos == 0) {
                    // It is zero of var int, not terminal.
                    return allocator.buffer(Byte.BYTES).writeByte(0);
                }

                boolean isNegative = value.isNegative();
                if (isNegative) {
                    if (nanos > 0) {
                        // Note: nanos should always be a positive integer or 0, see Duration.getNano().
                        // So the seconds should be humanity seconds - 1, so +1 then negate.
                        seconds = -(seconds + 1);
                        nanos = NANOS_OF_SECOND - nanos;
                    } else {
                        seconds = -seconds;
                    }
                }

                int size = nanos > 0 ? MICRO_TIME_SIZE : TIME_SIZE;

                ByteBuf buf = allocator.buffer(Byte.BYTES + size);

                try {
                    buf.writeByte(size)
                        .writeBoolean(isNegative)
                        .writeIntLE((int) (seconds / SECONDS_OF_DAY))
                        .writeByte((int) ((seconds % SECONDS_OF_DAY) / SECONDS_OF_HOUR))
                        .writeByte((int) ((seconds % SECONDS_OF_HOUR) / SECONDS_OF_MINUTE))
                        .writeByte((int) (seconds % SECONDS_OF_MINUTE));

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
            return Mono.fromRunnable(() -> encodeTo(writer));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.TIME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DurationParameter)) {
                return false;
            }

            DurationParameter that = (DurationParameter) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        private void encodeTo(ParameterWriter writer) {
            boolean isNegative = this.value.isNegative();
            Duration abs = this.value.abs();
            long totalSeconds = abs.getSeconds();
            int hours = (int) (totalSeconds / SECONDS_OF_HOUR);
            int minutes = (int) ((totalSeconds % SECONDS_OF_HOUR) / SECONDS_OF_MINUTE);
            int seconds = (int) (totalSeconds % SECONDS_OF_MINUTE);
            int micros = abs.getNano() / NANOS_OF_MICRO;

            if (hours < 0 || minutes < 0 || seconds < 0 || micros < 0) {
                throw new IllegalStateException(String.format(
                    "Duration %s abs value overflowing to %d:%02d:%02d.%06d", value, hours, minutes, seconds,
                    micros));
            }

            encodeTime(writer, isNegative, hours, minutes, seconds, micros);
        }
    }
}
