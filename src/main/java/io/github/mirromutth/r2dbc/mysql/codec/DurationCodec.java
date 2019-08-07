/*
 * Copyright 2018-2019 the original author or authors.
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

package io.github.mirromutth.r2dbc.mysql.codec;

import io.github.mirromutth.r2dbc.mysql.constant.BinaryDateTimes;
import io.github.mirromutth.r2dbc.mysql.constant.DataTypes;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.github.mirromutth.r2dbc.mysql.internal.CodecUtils;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

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
    public Duration decode(NormalFieldValue value, FieldInformation info, Class<? super Duration> target, boolean binary, MySqlSession session) {
        if (binary) {
            return decodeBinary(value.getBufferSlice());
        } else {
            return decodeText(value.getBufferSlice());
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Duration;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new DurationValue((Duration) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return DataTypes.TIME == info.getType();
    }

    private static Duration decodeText(ByteBuf buf) {
        boolean isNegative = LocalTimeCodec.readNegative(buf);
        int hour = CodecUtils.readIntInDigits(buf, true);
        int minute = CodecUtils.readIntInDigits(buf, true);
        int second = CodecUtils.readIntInDigits(buf, true);
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
    }
}
