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

/**
 * Codec for BIT, can convert to {@link boolean} if precision is 1.
 */
final class BooleanCodec extends AbstractPrimitiveCodec<Boolean> {

    static final BooleanCodec INSTANCE = new BooleanCodec();

    private BooleanCodec() {
        super(Boolean.TYPE, Boolean.class);
    }

    @Override
    public Boolean decodeText(NormalFieldValue value, FieldInformation info, Class<? super Boolean> target, MySqlSession session) {
        return value.getBuffer().readBoolean();
    }

    @Override
    public Boolean decodeBinary(NormalFieldValue value, FieldInformation info, Class<? super Boolean> target, MySqlSession session) {
        return value.getBuffer().readBoolean();
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Boolean;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return (Boolean) value ? BooleanValue.TRUE : BooleanValue.FALSE;
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataType.BIT == info.getType() && info.getSize() == 1;
    }

    private static final class BooleanValue extends AbstractParameterValue {

        private static final BooleanValue TRUE = new BooleanValue(true);

        private static final BooleanValue FALSE = new BooleanValue(false);

        private final boolean value;

        private BooleanValue(boolean value) {
            this.value = value;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeBoolean(value));
        }

        @Override
        public int getNativeType() {
            // Note: BIT will least 2-bytes in binary parameter (var integer size and content),
            // so use TINYINT will encode to buffer faster and shorter.
            return DataType.TINYINT.getType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BooleanValue)) {
                return false;
            }

            BooleanValue that = (BooleanValue) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return (value ? 1 : 0);
        }
    }
}
