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

/**
 * Codec for {@code short}.
 */
final class ShortCodec extends AbstractPrimitiveCodec<Short> {

    ShortCodec(ByteBufAllocator allocator) {
        super(allocator, Short.TYPE, Short.class);
    }

    @Override
    public Short decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        return (short) IntegerCodec.decodeInt(value, binary, metadata.getType());
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Short;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        short v = (Short) value;

        if ((byte) v == v) {
            return new ByteCodec.ByteParameter(allocator, (byte) v);
        }

        return new ShortParameter(allocator, v);
    }

    @Override
    public boolean canPrimitiveDecode(MySqlColumnMetadata metadata) {
        return metadata.getType().isNumeric();
    }

    static final class ShortParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final short value;

        ShortParameter(ByteBufAllocator allocator, short value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> allocator.buffer(Short.BYTES).writeShortLE(value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeInt(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.SMALLINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ShortParameter)) {
                return false;
            }

            ShortParameter that = (ShortParameter) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
