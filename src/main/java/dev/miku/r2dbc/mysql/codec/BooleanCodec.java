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

import dev.miku.r2dbc.mysql.ParameterOutputStream;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.Parameter;
import io.netty.buffer.ByteBuf;
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
    public Boolean decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        return value.readBoolean();
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Boolean;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return (Boolean) value ? BooleanParameter.TRUE : BooleanParameter.FALSE;
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataTypes.BIT == info.getType() && info.getSize() == 1;
    }

    private static final class BooleanParameter extends AbstractParameter {

        private static final BooleanParameter TRUE = new BooleanParameter(true);

        private static final BooleanParameter FALSE = new BooleanParameter(false);

        private final boolean value;

        private BooleanParameter(boolean value) {
            this.value = value;
        }

        @Override
        public Mono<Void> binary(ParameterOutputStream output) {
            return Mono.fromRunnable(() -> output.writeBoolean(value));
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeBinary(value));
        }

        @Override
        public short getType() {
            // Note: BIT will least 2-bytes in binary parameter (var integer size and content),
            // so use TINYINT will encode to buffer faster and shorter.
            return DataTypes.TINYINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BooleanParameter)) {
                return false;
            }

            BooleanParameter that = (BooleanParameter) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return (value ? 1 : 0);
        }
    }
}
