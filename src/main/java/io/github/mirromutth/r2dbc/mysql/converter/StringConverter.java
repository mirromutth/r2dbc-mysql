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
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.Set;

/**
 * Converter for {@link String}.
 */
final class StringConverter extends AbstractClassedConverter<String> {

    static final StringConverter INSTANCE = new StringConverter();

    private static final Set<ColumnType> STRING_TYPES = EnumSet.of(
        ColumnType.VARCHAR,
        ColumnType.STRING,
        ColumnType.VAR_STRING,
        ColumnType.ENUMERABLE,
        ColumnType.SET
    );

    private StringConverter() {
        super(String.class);
    }

    @Override
    public String read(ByteBuf buf, short definitions, int precision, int collationId, Class<? super String> target, MySqlSession session) {
        if (buf.readableBytes() <= 0) {
            return "";
        }

        return buf.toString(CharCollation.fromId(collationId, session.getServerVersion()).getCharset());
    }

    @Override
    boolean doCanRead(ColumnType type, short definitions) {
        return STRING_TYPES.contains(type);
    }
}
