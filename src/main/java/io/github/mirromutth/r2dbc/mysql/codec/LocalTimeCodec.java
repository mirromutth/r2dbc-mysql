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
import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.github.mirromutth.r2dbc.mysql.internal.CodecUtils;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

/**
 * Codec for {@link LocalTime}.
 */
final class LocalTimeCodec extends AbstractClassedCodec<LocalTime> {

    static final LocalTimeCodec INSTANCE = new LocalTimeCodec();

    private LocalTimeCodec() {
        super(LocalTime.class);
    }

    @Override
    public LocalTime decode(NormalFieldValue value, FieldInformation info, Class<? super LocalTime> target, boolean binary, MySqlSession session) {
        if (binary) {
            return decodeBinary(value.getBuffer());
        } else {
            return readTimeText(value.getBuffer());
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalTime;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new LocalTimeValue((LocalTime) value);
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataType.TIME == info.getType();
    }

    static LocalTime readTimeText(ByteBuf buf) {
        int hour = CodecUtils.readIntInDigits(buf, true);
        int minute = CodecUtils.readIntInDigits(buf, true);
        int second = CodecUtils.readIntInDigits(buf, true);

        // Time should always valid, no need check before construct.
        return LocalTime.of(hour, minute, second);
    }

    private static LocalTime decodeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < BinaryDateTimes.TIME_SIZE) {
            return LocalTime.MIDNIGHT;
        }

        // Skip sign and day.
        buf.skipBytes(Byte.BYTES + Integer.BYTES);

        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();

        if (bytes < BinaryDateTimes.MICRO_TIME_SIZE) {
            return LocalTime.of(hour, minute, second);
        }

        long micros = buf.readUnsignedIntLE();

        return LocalTime.of(hour, minute, second, (int) TimeUnit.MICROSECONDS.toNanos(micros));
    }

    private static final class LocalTimeValue extends AbstractParameterValue {

        private final LocalTime time;

        private LocalTimeValue(LocalTime time) {
            this.time = time;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeTime(time));
        }

        @Override
        public int getNativeType() {
            return DataType.TIME.getType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LocalTimeValue)) {
                return false;
            }

            LocalTimeValue that = (LocalTimeValue) o;

            return time.equals(that.time);
        }

        @Override
        public int hashCode() {
            return time.hashCode();
        }
    }
}
