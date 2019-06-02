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

import io.github.mirromutth.r2dbc.mysql.constant.ColumnDefinitions;
import io.github.mirromutth.r2dbc.mysql.constant.ColumnType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;

/**
 * Converter for {@link short}.
 */
final class ShortConverter extends AbstractPrimitiveConverter<Short> {

    static final ShortConverter INSTANCE = new ShortConverter();

    private ShortConverter() {
        super(Short.TYPE, Short.class);
    }

    @Override
    public Short read(ByteBuf buf, short definitions, int precision, int collationId, Class<? super Short> target, MySqlSession session) {
        return (short) IntegerConverter.parse(buf);
    }

    @Override
    boolean doCanRead(ColumnType type, short definitions, int precision) {
        return (definitions & ColumnDefinitions.UNSIGNED) != 0 ? ColumnType.TINYINT == type : ColumnType.SMALLINT == type;
    }
}
