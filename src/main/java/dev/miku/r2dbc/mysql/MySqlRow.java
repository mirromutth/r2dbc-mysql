/*
 * Copyright 2018-2020 the original author or authors.
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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.message.FieldValue;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.r2dbc.spi.Row;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

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
        requireNonNull(type, "type must not be null");

        MySqlColumnMetadata info = rowMetadata.getColumnMetadata(index);
        return codecs.decode(fields[index], info, type, binary, context);
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnMetadata info = rowMetadata.getColumnMetadata(name);
        return codecs.decode(fields[info.getIndex()], info, type, binary, context);
    }

    /**
     * @param index the column index starting at 0
     * @param type  must be {@link ParameterizedType} linked {@code T}
     * @param <T>   generic type, like {@code Set<String>}, {@code List<String>} or JSON-Serializable type when JSON serializer valid.
     * @return {@code type} specified generic instance.
     */
    @Nullable
    public <T> T get(int index, ParameterizedType type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnMetadata info = rowMetadata.getColumnMetadata(index);
        return codecs.decode(fields[index], info, type, binary, context);
    }

    @Nullable
    public <T> T get(String name, ParameterizedType type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnMetadata info = rowMetadata.getColumnMetadata(name);
        return codecs.decode(fields[info.getIndex()], info, type, binary, context);
    }
}
