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

/**
 * Codec for BIT, can convert to {@link boolean} if precision is 1.
 */
final class BooleanCodec extends AbstractPrimitiveCodec<Boolean> {

    BooleanCodec(ByteBufAllocator allocator) {
        super(allocator, Boolean.TYPE, Boolean.class);
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
        return new BooleanParameter(allocator, (Boolean) value);
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataTypes.BIT == info.getType() && info.getSize() == 1;
    }

    private static final class BooleanParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final boolean value;

        private BooleanParameter(ByteBufAllocator allocator, boolean value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> allocator.buffer(Byte.BYTES).writeByte(value ? 1 : 0));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
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
