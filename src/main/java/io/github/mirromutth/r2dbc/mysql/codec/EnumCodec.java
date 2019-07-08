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

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.FieldValue;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
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
    public Enum<?> decode(NormalFieldValue value, FieldInformation info, Class<?> target, boolean binary, MySqlSession session) {
        Charset charset = CharCollation.fromId(info.getCollationId(), session.getServerVersion()).getCharset();
        @SuppressWarnings("unchecked")
        Enum<?> e = Enum.valueOf((Class<Enum>) target, value.getBuffer().toString(charset));
        return e;
    }

    @Override
    public boolean canDecode(FieldValue value, FieldInformation info, Type target) {
        if (DataType.ENUMERABLE == info.getType() && target instanceof Class<?> && value instanceof NormalFieldValue) {
            return ((Class<?>) target).isEnum();
        }

        return false;
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Enum<?> && value.getClass().isEnum();
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new EnumValue((Enum<?>) value, session);
    }

    private static final class EnumValue extends AbstractParameterValue {

        private final Enum<?> value;

        private final MySqlSession session;

        private EnumValue(Enum<?> value, MySqlSession session) {
            this.value = value;
            this.session = session;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeCharSequence(value.name(), session.getCollation()));
        }

        @Override
        public int getNativeType() {
            return DataType.VARCHAR.getType();
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
