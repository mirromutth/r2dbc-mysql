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
 * Converter for {@link byte}.
 */
final class ByteConverter extends AbstractPrimitiveConverter<Byte> {

    static final ByteConverter INSTANCE = new ByteConverter();

    private ByteConverter() {
        super(Byte.TYPE, Byte.class);
    }

    @Override
    public Byte read(ByteBuf buf, short definitions, int precision, int collationId, Class<? super Byte> target, MySqlSession session) {
        return (byte) IntegerConverter.parse(buf);
    }

    @Override
    boolean doCanRead(ColumnType type, short definitions, int precision) {
        boolean isUnsigned = (definitions & ColumnDefinitions.UNSIGNED) != 0;
        return (!isUnsigned && ColumnType.TINYINT == type) || (ColumnType.BIT == type && precision <= Byte.SIZE);
    }
}
