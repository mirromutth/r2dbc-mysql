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

import io.github.mirromutth.r2dbc.mysql.codec.Codecs;
import io.github.mirromutth.r2dbc.mysql.internal.ConnectionContext;
import io.github.mirromutth.r2dbc.mysql.message.FieldValue;
import io.r2dbc.spi.Row;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Row} for MySQL database.
 */
public final class MySqlRow implements Row {

    private final FieldValue[] fields;

    private final MySqlRowMetadata rowMetadata;

    private final Codecs codecs;

    /**
     * It is binary decode logic.
     */
    private final boolean binary;

    private final ConnectionContext context;

    MySqlRow(FieldValue[] fields, MySqlRowMetadata rowMetadata, Codecs codecs, boolean binary, ConnectionContext context) {
        this.fields = requireNonNull(fields, "fields must not be null");
        this.rowMetadata = requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.binary = binary;
        this.context = requireNonNull(context, "context must not be null");
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        return get0(index, type);
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        return get0(name, type);
    }

    /**
     * @param index the column index starting at 0
     * @param type  must be {@link ParameterizedType} linked {@code T}
     * @param <T>   generic type, like {@code Set<String>}, {@code List<String>} or JSON-Serializable type when JSON serializer valid.
     * @return {@code type} specified generic instance.
     */
    @Nullable
    public <T> T get(int index, ParameterizedType type) {
        return get0(index, type);
    }

    @Nullable
    public <T> T get(String name, ParameterizedType type) {
        return get0(name, type);
    }

    @Nullable
    private <T> T get0(int index, Type type) {
        requireNonNull(type, "type must not be null");

        return codecs.decode(fields[index], rowMetadata.getColumnMetadata(index), type, binary, context);
    }

    @Nullable
    private <T> T get0(String name, Type type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnMetadata info = rowMetadata.getColumnMetadata(name);
        return codecs.decode(fields[info.getIndex()], info, type, binary, context);
    }
}
