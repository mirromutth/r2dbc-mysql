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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Codec for {@link Duration}.
 */
final class DurationCodec extends AbstractClassedCodec<Duration> {

    DurationCodec(ByteBufAllocator allocator) {
        super(allocator, Duration.class);
    }

    @Override
    public Duration decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        if (binary) {
            return decodeBinary(value);
        } else {
            return decodeText(value);
        }
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
    protected boolean doCanDecode(FieldInformation info) {
        return DataTypes.TIME == info.getType();
    }

    static void encodeTime(ParameterWriter writer, boolean isNegative, int hours, int minutes, int seconds, int micros) {
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
            // WATCH OUT for inf loop: i from 100000 to 1, micros is greater than 0,
            // 0 < micros < 1 is impossible, so micros < 1 will be false finally,
            // then loop done. Safe.
            for (int i = 100000; micros < i; i /= 10) {
                writer.append('0');
            }
            // WATCH OUT for inf loop: micros is greater than 0, that means it least
            // contains one digit which is not 0, so micros % 10 == 0 will be false
            // finally, then loop done. Safe.
            while (micros % 10 == 0) {
                micros /= 10;
            }
            writer.writeInt(micros);
        }
    }

    private static Duration decodeText(ByteBuf buf) {
        boolean isNegative = LocalTimeCodec.readNegative(buf);
        int hour = DateTimes.readIntInDigits(buf);
        int minute = DateTimes.readIntInDigits(buf);
        int second = DateTimes.readIntInDigits(buf);
        long totalSeconds = TimeUnit.HOURS.toSeconds(hour) + TimeUnit.MINUTES.toSeconds(minute) + second;

        return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds);
    }

    private static Duration decodeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < DateTimes.TIME_SIZE) {
            return Duration.ZERO;
        }

        boolean isNegative = buf.readBoolean();

        long day = buf.readUnsignedIntLE();
        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();
        long totalSeconds = TimeUnit.DAYS.toSeconds(day) +
            TimeUnit.HOURS.toSeconds(hour) +
            TimeUnit.MINUTES.toSeconds(minute) +
            second;

        if (bytes < DateTimes.MICRO_TIME_SIZE) {
            return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds);
        }

        long nanos = TimeUnit.MICROSECONDS.toNanos(buf.readUnsignedIntLE());

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
        public Mono<ByteBuf> binary() {
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
                        // So if duration is negative, seconds should be humanity seconds - 1, so +1 then negate.
                        seconds = -(seconds + 1);
                        nanos = DateTimes.NANOS_OF_SECOND - nanos;
                    } else {
                        seconds = -seconds;
                    }
                }

                int size = nanos > 0 ? DateTimes.MICRO_TIME_SIZE : DateTimes.TIME_SIZE;

                ByteBuf buf = allocator.buffer(Byte.BYTES + size);

                try {
                    buf.writeByte(size)
                        .writeBoolean(isNegative)
                        .writeIntLE((int) (seconds / DateTimes.SECONDS_OF_DAY))
                        .writeByte((int) ((seconds % DateTimes.SECONDS_OF_DAY) / DateTimes.SECONDS_OF_HOUR))
                        .writeByte((int) ((seconds % DateTimes.SECONDS_OF_HOUR) / DateTimes.SECONDS_OF_MINUTE))
                        .writeByte((int) (seconds % DateTimes.SECONDS_OF_MINUTE));

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
            return Mono.fromRunnable(() -> encodeTo(writer));
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
            int hours = (int) (totalSeconds / 3600);
            int minutes = (int) ((totalSeconds / 60) % 60);
            int seconds = (int) (totalSeconds % 60);
            int micros = (int) TimeUnit.NANOSECONDS.toMicros(abs.getNano());

            if (hours < 0 || minutes < 0 || seconds < 0 || micros < 0) {
                throw new IllegalStateException(String.format("Too large duration %s, abs value overflowing to %02d:%02d:%02d.%06d", value, hours, minutes, seconds, micros));
            }

            encodeTime(writer, isNegative, hours, minutes, seconds, micros);
        }
    }
}
