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
import io.github.mirromutth.r2dbc.mysql.json.MySqlJson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Type;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Converters}.
 */
final class DefaultConverters implements Converters {

    private final Converter<?, ?>[] converters;

    DefaultConverters(Converter<?, ?>... converters) {
        this.converters = requireNonNull(converters, "converters must not be null");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T read(@Nullable ByteBuf buf, @Nullable ColumnType columnType, boolean isUnsigned, int precision, int collationId, Type targetType, MySqlSession session) {
        if (buf == null) {
            return null;
        }

        try {
            requireNonNull(session, "session must not be null");
            requireNonNull(targetType, "targetType must not be null");

            if (columnType == null) {
                // Unknown column type, try convert to bytes
                return convertToBytes(buf, targetType);
            }

            for (Converter<?, ?> converter : converters) {
                if (converter.canRead(columnType, isUnsigned, precision, collationId, targetType, session)) {
                    return ((Converter<T, ? super Type>) converter).read(buf, isUnsigned, precision, collationId, targetType, session);
                }
            }

            throw new IllegalArgumentException("Cannot decode value of type " + targetType);
        } finally {
            buf.release();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T convertToBytes(ByteBuf buf, Type targetType) {
        if (targetType instanceof Class<?>) {
            Class<?> targetClass = (Class<?>) targetType;
            // include targetType is Object.class
            if (targetClass.isAssignableFrom(byte[].class)) {
                return (T) ByteBufUtil.getBytes(buf);
            } else if (targetClass.isAssignableFrom(ByteBuf.class)) {
                // can NOT expose the internal ByteBuf
                return (T) buf.copy();
            }
        }

        throw new IllegalArgumentException("Cannot decode value of type " + targetType);
    }
}
