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

package io.github.mirromutth.r2dbc.mysql.converter;

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.ColumnType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;
import java.nio.charset.Charset;

/**
 * Converter for all {@code enum class}.
 */
final class EnumConverter implements Converter<Enum<?>, Class<Enum<?>>> {

    static final EnumConverter INSTANCE = new EnumConverter();

    private EnumConverter() {
    }

    @Override
    public Enum<?> read(ByteBuf buf, short definitions, int precision, int collationId, Class<Enum<?>> target, MySqlSession session) {
        Charset charset = CharCollation.fromId(collationId, session.getServerVersion()).getCharset();
        @SuppressWarnings("unchecked")
        Enum<?> e = Enum.valueOf((Class<Enum>) (Class<?>) target, buf.toString(charset));
        return e;
    }

    @Override
    public boolean canRead(ColumnType type, short definitions, int precision, int collationId, Type target, MySqlSession session) {
        if (ColumnType.ENUMERABLE == type && target instanceof Class<?>) {
            return ((Class<?>) target).isEnum();
        }

        return false;
    }
}
