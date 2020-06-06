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
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;

/**
 * Codec for {@code enum class}.
 */
final class EnumCodec implements Codec<Enum<?>> {

    private final ByteBufAllocator allocator;

    EnumCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Enum<?> decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        Charset charset = CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset();
        return Enum.valueOf((Class<Enum>) target, value.toString(charset));
    }

    @Override
    public boolean canDecode(FieldInformation info, Class<?> target) {
        return DataTypes.ENUMERABLE == info.getType() && target.isEnum();
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Enum<?>;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new EnumParameter(allocator, (Enum<?>) value, context);
    }

    private static final class EnumParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final Enum<?> value;

        private final CodecContext context;

        private EnumParameter(ByteBufAllocator allocator, Enum<?> value, CodecContext context) {
            this.allocator = allocator;
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> StringCodec.encodeCharSequence(allocator, value.name(), context));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.write(value.name()));
        }

        @Override
        public short getType() {
            return DataTypes.VARCHAR;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EnumParameter)) {
                return false;
            }

            EnumParameter enumValue = (EnumParameter) o;

            return value.equals(enumValue.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
