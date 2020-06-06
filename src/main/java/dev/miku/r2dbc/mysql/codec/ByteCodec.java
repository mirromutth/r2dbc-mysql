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
import dev.miku.r2dbc.mysql.constant.ColumnDefinitions;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

/**
 * Codec for {@link byte}.
 */
final class ByteCodec extends AbstractPrimitiveCodec<Byte> {

    ByteCodec(ByteBufAllocator allocator) {
        super(allocator, Byte.TYPE, Byte.class);
    }

    @Override
    public Byte decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        if (binary) {
            return value.readByte();
        } else {
            return (byte) IntegerCodec.parse(value);
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Byte;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new ByteParameter(allocator, (Byte) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return DataTypes.TINYINT == info.getType() && (info.getDefinitions() & ColumnDefinitions.UNSIGNED) == 0;
    }

    static final class ByteParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final byte value;

        ByteParameter(ByteBufAllocator allocator, byte value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> allocator.buffer(Byte.BYTES).writeByte(value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeInt(value));
        }

        @Override
        public short getType() {
            return DataTypes.TINYINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ByteParameter)) {
                return false;
            }

            ByteParameter byteValue = (ByteParameter) o;

            return value == byteValue.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
