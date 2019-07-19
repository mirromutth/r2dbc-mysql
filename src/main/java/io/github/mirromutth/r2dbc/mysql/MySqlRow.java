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
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.FieldValue;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Row;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Row} for MySQL database.
 */
public final class MySqlRow extends AbstractReferenceCounted implements Row {

    private final FieldValue[] fields;

    private final MySqlRowMetadata rowMetadata;

    private final Codecs codecs;

    /**
     * It is binary decode logic.
     */
    private final boolean binary;

    private final MySqlSession session;

    MySqlRow(FieldValue[] fields, MySqlRowMetadata rowMetadata, Codecs codecs, boolean binary, MySqlSession session) {
        this.fields = requireNonNull(fields, "fields must not be null");
        this.rowMetadata = requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.binary = binary;
        this.session = requireNonNull(session, "session must not be null");
    }

    @Override
    public <T> T get(Object identifier, Class<T> type) {
        return getByType(identifier, type);
    }

    /**
     * @param identifier must be {@link Integer} or {@link String}, means field ordinal or field name.
     * @param type must be {@link ParameterizedType} linked {@code T}
     * @param <T> generic type, like {@code Set<String>}, {@code List<String>} or JSON-Serializable type when JSON serializer valid.
     * @return {@code type} specified generic instance.
     */
    @Nullable
    public <T> T get(Object identifier, ParameterizedType type) {
        return getByType(identifier, type);
    }

    @Override
    public MySqlRow touch(@Nullable Object hint) {
        for (FieldValue field : fields) {
            if (field != null) {
                field.touch(hint);
            }
        }

        return this;
    }

    @Override
    protected void deallocate() {
        for (FieldValue field : fields) {
            ReferenceCountUtil.safeRelease(field);
        }
    }

    @Nullable
    private <T> T getByType(Object identifier, Type type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnMetadata info = rowMetadata.getColumnMetadata(identifier);
        FieldValue field = fields[info.getIndex()];

        return codecs.decode(field, info, type, binary, session);
    }
}
