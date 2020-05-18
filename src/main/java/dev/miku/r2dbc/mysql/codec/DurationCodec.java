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

import dev.miku.r2dbc.mysql.constant.BinaryDateTimes;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Codec for {@link Duration}.
 */
final class DurationCodec extends AbstractClassedCodec<Duration> {

    static final DurationCodec INSTANCE = new DurationCodec();

    private DurationCodec() {
        super(Duration.class);
    }

    @Override
    public Duration decode(ByteBuf value, FieldInformation info, Type target, boolean binary, ConnectionContext context) {
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
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new DurationValue((Duration) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return DataTypes.TIME == info.getType();
    }

    static void encodeTime(StringBuilder builder, boolean isNegative, int hours, int minutes, int seconds, int micros) {
        if (isNegative) {
            builder.append('-');
        }

        if (hours < 10) {
            builder.append('0');
        }

        builder.append(hours).append(':');

        if (minutes < 10) {
            builder.append('0');
        }

        builder.append(minutes).append(':');

        if (seconds < 10) {
            builder.append('0');
        }

        builder.append(seconds);

        // Must be greater than 0, can NOT use "micros != 0" here.
        // Sure, micros will never less than 0, but need to check for avoid inf loop.
        if (micros > 0) {
            builder.append('.');
            // WATCH OUT for inf loop: i from 100000 to 1, micros is greater than 0,
            // 0 < micros < 1 is impossible, so micros < 1 will be false finally,
            // then loop done. Safe.
            for (int i = 100000; micros < i; i /= 10) {
                builder.append('0');
            }
            // WATCH OUT for inf loop: micros is greater than 0, that means it least
            // contains one digit which is not 0, so micros % 10 == 0 will be false
            // finally, then loop done. Safe.
            while (micros % 10 == 0) {
                micros /= 10;
            }
            builder.append(micros);
        }
    }

    private static Duration decodeText(ByteBuf buf) {
        boolean isNegative = LocalTimeCodec.readNegative(buf);
        int hour = CodecDateUtils.readIntInDigits(buf);
        int minute = CodecDateUtils.readIntInDigits(buf);
        int second = CodecDateUtils.readIntInDigits(buf);
        long totalSeconds = TimeUnit.HOURS.toSeconds(hour) + TimeUnit.MINUTES.toSeconds(minute) + second;

        return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds);
    }

    private static Duration decodeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < BinaryDateTimes.TIME_SIZE) {
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

        if (bytes < BinaryDateTimes.MICRO_TIME_SIZE) {
            return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds);
        }

        long nanos = TimeUnit.MICROSECONDS.toNanos(buf.readUnsignedIntLE());

        return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds, isNegative ? -nanos : nanos);
    }

    private static final class DurationValue extends AbstractParameterValue {

        private final Duration value;

        private DurationValue(Duration value) {
            this.value = value;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeDuration(value));
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.fromRunnable(() -> encodeTo(builder));
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
            if (!(o instanceof DurationValue)) {
                return false;
            }

            DurationValue that = (DurationValue) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        private void encodeTo(StringBuilder builder) {
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

            builder.append('\'');
            encodeTime(builder, isNegative, hours, minutes, seconds, micros);
            builder.append('\'');
        }
    }
}
