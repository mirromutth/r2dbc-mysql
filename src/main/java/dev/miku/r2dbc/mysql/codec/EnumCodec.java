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

import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import dev.miku.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;

/**
 * Codec for {@code enum class}.
 */
final class EnumCodec implements Codec<Enum<?>> {

    static final EnumCodec INSTANCE = new EnumCodec();

    private EnumCodec() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Enum<?> decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        Charset charset = CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset();
        return Enum.valueOf((Class<Enum>) target, value.toString(charset));
    }

    @Override
    public boolean canDecode(boolean massive, FieldInformation info, Class<?> target) {
        if (DataTypes.ENUMERABLE == info.getType() && !massive) {
            return target.isEnum();
        }

        return false;
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Enum<?>;
    }

    @Override
    public ParameterValue encode(Object value, CodecContext context) {
        return new EnumValue((Enum<?>) value, context);
    }

    private static final class EnumValue extends AbstractParameterValue {

        private final Enum<?> value;

        private final CodecContext context;

        private EnumValue(Enum<?> value, CodecContext context) {
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeCharSequence(value.name(), context.getClientCollation()));
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.fromRunnable(() -> {
                builder.append('\'');
                // Java will be not including special character in enum names,
                // but other JVM languages may be that, so must escape string here.
                CodecUtils.appendEscape(builder, value.name());
                builder.append('\'');
            });
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
            if (!(o instanceof EnumValue)) {
                return false;
            }

            EnumValue enumValue = (EnumValue) o;

            return value.equals(enumValue.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
