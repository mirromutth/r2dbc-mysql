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
import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;

/**
 * Converter only for {@link Byte[]}, excludes other types of all.
 */
final class BoxedByteArrayConverter implements Converter<Byte[], Class<Byte[]>> {

    static final BoxedByteArrayConverter INSTANCE = new BoxedByteArrayConverter();

    private BoxedByteArrayConverter() {
    }

    @Override
    public Byte[] read(ByteBuf buf, boolean isUnsigned, int precision, int collationId, Class<Byte[]> target, MySqlSession session) {
        int size = buf.readableBytes();
        Byte[] result = new Byte[size];

        for (int i = 0; i < size; ++i) {
            result[i] = buf.readByte();
        }

        return result;
    }

    @Override
    public boolean canRead(ColumnType type, boolean isUnsigned, int precision, int collationId, Type target, MySqlSession session) {
        return Byte[].class == target;
    }
}
