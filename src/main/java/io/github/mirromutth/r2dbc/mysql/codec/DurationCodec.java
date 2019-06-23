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

import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Codec for {@link Duration}.
 */
final class DurationCodec extends AbstractClassedCodec<Duration> {

    static final DurationCodec INSTANCE = new DurationCodec();

    private DurationCodec() {
        super(Duration.class);
    }

    @Override
    public Duration decodeText(NormalFieldValue value, FieldInformation info, Class<? super Duration> target, MySqlSession session) {
        return JavaTimeHelper.readDurationText(value.getBuffer());
    }

    @Override
    public Duration decodeBinary(NormalFieldValue value, FieldInformation info, Class<? super Duration> target, MySqlSession session) {
        return JavaTimeHelper.readDurationBinary(value.getBuffer());
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
        return DataType.TIME == info.getType();
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
        public int getNativeType() {
            return DataType.TIME.getType();
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
