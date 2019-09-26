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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.FieldValue;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.nio.charset.Charset;

/**
 * Codec for {@code enum class}.
 */
final class EnumCodec implements Codec<Enum<?>, NormalFieldValue, Class<?>> {

    static final EnumCodec INSTANCE = new EnumCodec();

    private EnumCodec() {
    }

    @Override
    public Enum<?> decode(NormalFieldValue value, FieldInformation info, Class<?> target, boolean binary, ConnectionContext context) {
        Charset charset = CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset();
        @SuppressWarnings("unchecked")
        Enum<?> e = Enum.valueOf((Class<Enum>) target, value.getBufferSlice().toString(charset));
        return e;
    }

    @Override
    public boolean canDecode(FieldValue value, FieldInformation info, Type target) {
        if (DataTypes.ENUMERABLE == info.getType() && target instanceof Class<?> && value instanceof NormalFieldValue) {
            return ((Class<?>) target).isEnum();
        }

        return false;
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Enum<?> && value.getClass().isEnum();
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new EnumValue((Enum<?>) value, context);
    }

    private static final class EnumValue extends AbstractParameterValue {

        private final Enum<?> value;

        private final ConnectionContext context;

        private EnumValue(Enum<?> value, ConnectionContext context) {
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeCharSequence(value.name(), context.getCollation()));
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
