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
 * Converter for {@link long}.
 */
final class LongConverter extends AbstractPrimitiveConverter<Long> {

    static final LongConverter INSTANCE = new LongConverter();

    private LongConverter() {
        super(Long.TYPE, Long.class);
    }

    @Override
    public Long read(ByteBuf buf, short definitions, int precision, int collationId, Class<? super Long> target, MySqlSession session) {
        long result = parse(buf);

        if (result < 0 && (definitions & ColumnDefinitions.UNSIGNED) != 0) {
            // INT UNSIGNED can not overflow, so it must be LONG UNSIGNED.
            throw new ArithmeticException("long overflow");
        }

        return result;
    }

    @Override
    boolean doCanRead(ColumnType type, short definitions, int precision) {
        // Here is a special judgment. In a realistic application scenario, many times programmers define
        // BIGINT UNSIGNED usually for make sure the ID is not negative, in fact they just use 63-bits.
        // If users force the requirement to convert BIGINT UNSIGNED to Long, should allow this behavior
        // for better performance (BigInteger is obviously slower than long).
        return ColumnType.BIGINT == type || (ColumnType.INT == type && ((definitions & ColumnDefinitions.UNSIGNED) != 0));
    }

    /**
     * Fast parse a negotiable integer from {@link ByteBuf} without copy.
     *
     * @param buf a {@link ByteBuf} include a integer that maybe has sign.
     * @return a integer from {@code buf}.
     */
    private static long parse(ByteBuf buf) {
        long value = 0;
        int first = buf.readByte();
        final boolean isNegative;

        if (first == '-') {
            isNegative = true;
        } else if (first >= '0' && first <= '9') {
            isNegative = false;
            value = (long) (first - '0');
        } else {
            // must be '+'
            isNegative = false;
        }

        while (buf.isReadable()) {
            value = value * 10L + (buf.readByte() - '0');
        }

        if (isNegative) {
            return -value;
        } else {
            return value;
        }
    }
}
