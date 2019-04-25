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

package io.github.mirromutth.r2dbc.mysql;

import io.github.mirromutth.r2dbc.mysql.converter.Converters;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.netty.buffer.ByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.Row;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requirePositive;

/**
 * An implementation of {@link Row} for MySQL database.
 */
final class MySqlRow implements Row, ReferenceCounted {

    /**
     * WARNING: elements maybe null.
     */
    private final List<ByteBuf> fields;

    private final MySqlRowMetadata rowMetadata;

    private final Converters converters;

    private final MySqlSession session;

    private final AtomicInteger refCnt = new AtomicInteger(1);

    public MySqlRow(List<ByteBuf> fields, MySqlRowMetadata rowMetadata, Converters converters, MySqlSession session) {
        this.fields = requireNonNull(fields, "fields must not be null");
        this.rowMetadata = requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.converters = requireNonNull(converters, "converters must not be null");
        this.session = requireNonNull(session, "session must not be null");
    }

    @Override
    public <T> T get(Object identifier, Class<T> type) {
        return getByType(identifier, type);
    }

    /**
     * @param identifier must be {@link Integer} or {@link String}, means field ordinal or field name.
     * @param type must be {@link ParameterizedType} linked {@code T}
     * @param <T> generic type, like {@code Set<String>}, {@code List<String>} or JSON-Serializable type when JSON serializer not be null.
     * @return {@code type} specified generic instance.
     */
    @Nullable
    public <T> T get(Object identifier, ParameterizedType type) {
        return getByType(identifier, type);
    }

    @Override
    public int refCnt() {
        return refCnt.get();
    }

    @Override
    public MySqlRow retain() {
        return retain0(1);
    }

    @Override
    public MySqlRow retain(int increment) {
        return retain0(requirePositive(increment, "increment must be a positive integer"));
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override
    public MySqlRow touch() {
        // Use the old-style for loop, see: https://github.com/netty/netty/issues/2642
        int size = fields.size();

        for (int i = 0; i < size; ++i) {
            fields.get(i).touch();
        }

        return this;
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override
    public MySqlRow touch(Object hint) {
        // Use the old-style for loop, see: https://github.com/netty/netty/issues/2642
        int size = fields.size();

        for (int i = 0; i < size; ++i) {
            fields.get(i).touch(hint);
        }

        return this;
    }

    @Override
    public boolean release() {
        return release0(1);
    }

    @Override
    public boolean release(int decrement) {
        return release0(requirePositive(decrement, "decrement must be a positive integer"));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Nullable
    private <T> T getByType(Object identifier, Type type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnMetadata columnMetadata = rowMetadata.getColumnMetadata(identifier);
        ByteBuf field;

        if (identifier instanceof Integer) {
            field = fields.get((Integer) identifier);
        } else {
            field = fields.get(columnMetadata.getOrdinal());
        }

        return converters.read(
            field,
            columnMetadata.getType(),
            columnMetadata.isUnsigned(),
            columnMetadata.getCollationId(),
            columnMetadata.getPrecision(),
            chooseTarget(columnMetadata, type),
            session
        );
    }

    private Type chooseTarget(MySqlColumnMetadata columnMetadata, Type targetType) {
        // Object.class means could return any thing
        if (targetType == Object.class) {
            Class<?> mainType = columnMetadata.getJavaType();

            if (mainType != null) {
                // use main type if main type exists
                return mainType;
            }
            // otherwise no main type, just use Object.class
        }

        return targetType;
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void freeAll() {
        // Use the old-style for loop, see: https://github.com/netty/netty/issues/2642
        int size = fields.size();

        for (int i = 0; i < size; ++i) {
            fields.get(i).release();
        }
    }

    private MySqlRow retain0(int increment) {
        int oldRef = this.refCnt.getAndAdd(increment);

        if (oldRef <= 0 || oldRef + increment < oldRef) {
            this.refCnt.getAndAdd(-increment);
            throw new IllegalReferenceCountException(oldRef, increment);
        }

        return this;
    }

    private boolean release0(int decrement) {
        int oldRef = this.refCnt.getAndAdd(-decrement);

        if (oldRef == decrement) {
            freeAll();
            return true;
        }

        if (oldRef < decrement || oldRef - decrement > oldRef) {
            this.refCnt.getAndAdd(decrement);
            throw new IllegalReferenceCountException(oldRef, -decrement);
        }

        return false;
    }
}
