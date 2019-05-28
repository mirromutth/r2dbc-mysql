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
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.util.EmptyArrays;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;

/**
 * Converter for binary data.
 */
final class BinaryConverter implements Converter<Object, Class<?>> {

    static final BinaryConverter INSTANCE = new BinaryConverter();

    private BinaryConverter() {
    }

    @Override
    public Object read(ByteBuf buf, short definitions, int precision, int collationId, Class<?> target, MySqlSession session) {
        if (target.isAssignableFrom(byte[].class)) {
            return convertBytes(buf);
        } else if (target.isAssignableFrom(ByteBuf.class)) {
            if (buf.readableBytes() == 0) {
                return buf.alloc().buffer(0, 0);
            }

            return buf.copy();
        } else if (target.isAssignableFrom(ByteBuffer.class)) {
            return ByteBuffer.wrap(convertBytes(buf));
        } else if (Byte[].class == target) {
            int size = buf.readableBytes();
            Byte[] result = new Byte[size];

            for (int i = 0; i < size; ++i) {
                result[i] = buf.readByte();
            }

            return result;
        } else {
            throw new IllegalArgumentException(target + " is not support by binary data");
        }
    }

    @Override
    public boolean canRead(ColumnType type, short definitions, int precision, int collationId, Type target, MySqlSession session) {
        // BIT use BitsConverter, do not use this.
        if (ColumnType.BIT == type || !(target instanceof Class<?>)) {
            return false;
        }

        Class<?> targetClass = (Class<?>) target;

        return targetClass.isAssignableFrom(byte[].class) ||
            targetClass.isAssignableFrom(ByteBuf.class) ||
            targetClass.isAssignableFrom(ByteBuffer.class) ||
            Byte[].class == target;
    }

    private byte[] convertBytes(ByteBuf buf) {
        if (buf.readableBytes() == 0) {
            return EmptyArrays.EMPTY_BYTES;
        }

        return ByteBufUtil.getBytes(buf);
    }
}
