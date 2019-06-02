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

import io.github.mirromutth.r2dbc.mysql.constant.ColumnType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.lang.reflect.Type;
import java.util.BitSet;

/**
 * Converter for BIT, can convert to {@link boolean} if precision is 1.
 */
final class BitsConverter implements Converter<Object, Class<?>> {

    static final BitsConverter INSTANCE = new BitsConverter();

    private BitsConverter() {
    }

    @Override
    public Object read(ByteBuf buf, short definitions, int precision, int collationId, Class<?> target, MySqlSession session) {
        if (target == byte[].class) {
            return ByteBufUtil.getBytes(buf);
        } else if (precision == 1 && isBooleanType(target)) {
            return buf.readByte() != 0;
        } else {
            return BitSet.valueOf(revert(ByteBufUtil.getBytes(buf)));
        }
    }

    @Override
    public boolean canRead(ColumnType type, short definitions, int precision, int collationId, Type target, MySqlSession session) {
        if (ColumnType.BIT == type && target instanceof Class<?>) {
            Class<?> targetClass = (Class<?>) target;
            return targetClass == byte[].class ||
                (precision == 1 && isBooleanType(targetClass)) ||
                targetClass.isAssignableFrom(BitSet.class);
        }

        return false;
    }

    private static boolean isBooleanType(Class<?> targetClass) {
        return Boolean.TYPE == targetClass || targetClass.isAssignableFrom(Boolean.class);
    }

    private static byte[] revert(byte[] bytes) {
        int maxIndex = bytes.length - 1;
        int half = bytes.length >>> 1;

        for (int i = 0; i < half; ++i) {
            bytes[i] = bytes[maxIndex - i];
        }

        return bytes;
    }
}
