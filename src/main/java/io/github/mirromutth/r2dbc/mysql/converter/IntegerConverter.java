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
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.netty.buffer.ByteBuf;

/**
 * Converter for {@link int}.
 */
final class IntegerConverter extends AbstractPrimitiveConverter<Integer> {

    static final IntegerConverter INSTANCE = new IntegerConverter();

    private IntegerConverter() {
        super(Integer.TYPE, Integer.class);
    }

    @Override
    public Integer read(ByteBuf buf, short definitions, int precision, int collationId, Class<? super Integer> target, MySqlSession session) {
        return parse(buf);
    }

    @Override
    boolean doCanRead(ColumnType type, short definitions, int precision) {
        boolean isUnsigned = (definitions & ColumnDefinitions.UNSIGNED) != 0;
        return (!isUnsigned && ColumnType.INT == type) || ColumnType.MEDIUMINT == type;
    }

    /**
     * Fast parse a negotiable integer from {@link ByteBuf} without copy.
     *
     * @param buf a {@link ByteBuf} include a integer that maybe has sign.
     * @return a integer from {@code buf}.
     */
    static int parse(ByteBuf buf) {
        int value = 0;
        int first = buf.readByte();
        final boolean isNegative;

        if (first == '-') {
            isNegative = true;
        } else if (first >= '0' && first <= '9') {
            isNegative = false;
            value = first - '0';
        } else {
            // must be '+'
            isNegative = false;
        }

        while (buf.isReadable()) {
            value = value * 10 + (buf.readByte() - '0');
        }

        return isNegative ? -value : value;
    }
}
