/*
 * Copyright 2018-2021 the original author or authors.
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
import dev.miku.r2dbc.mysql.constant.MySqlType;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Row} for support to reading last inserted ID.
 * <p>
 * It is also an implementation of {@link RowMetadata} and {@link ColumnMetadata} for reduce redundant
 * objects.
 *
 * @see MySqlStatement#returnGeneratedValues(String...) reading last inserted ID.
 */
final class InsertSyntheticRow implements Row, RowMetadata, ColumnMetadata {

    private final Codecs codecs;

    private final String keyName;

    private final long lastInsertId;

    private final ColumnNameSet nameSet;

    InsertSyntheticRow(Codecs codecs, String keyName, long lastInsertId) {
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.keyName = requireNonNull(keyName, "keyName must not be null");
        // lastInsertId may be negative if key is BIGINT UNSIGNED and value overflow than signed int64.
        this.lastInsertId = lastInsertId;
        // Singleton name must be sorted.
        this.nameSet = ColumnNameSet.of(keyName);
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        requireNonNull(type, "type must not be null");
        assertValidIndex(index);

        return get0(type);
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        requireNonNull(name, "name must not be null");
        requireNonNull(type, "type must not be null");
        assertValidName(name);

        return get0(type);
    }

    @Override
    public Number get(int index) {
        assertValidIndex(index);

        return get0(getType().getJavaType());
    }

    @Override
    public Number get(String name) {
        requireNonNull(name, "name must not be null");
        assertValidName(name);

        return get0(getType().getJavaType());
    }

    @Override
    public boolean contains(String name) {
        requireNonNull(name, "name must not be null");

        return contains0(name);
    }

    @Override
    public RowMetadata getMetadata() {
        return this;
    }

    @Override
    public ColumnMetadata getColumnMetadata(int index) {
        assertValidIndex(index);

        return this;
    }

    @Override
    public ColumnMetadata getColumnMetadata(String name) {
        requireNonNull(name, "name must not be null");
        assertValidName(name);

        return this;
    }

    @Override
    public List<ColumnMetadata> getColumnMetadatas() {
        return Collections.singletonList(this);
    }

    @Override
    @SuppressWarnings("deprecation")
    public Set<String> getColumnNames() {
        return nameSet;
    }

    @Override
    public MySqlType getType() {
        return lastInsertId < 0 ? MySqlType.BIGINT_UNSIGNED : MySqlType.BIGINT;
    }

    @Override
    public String getName() {
        return keyName;
    }

    @Override
    public Class<?> getJavaType() {
        return getType().getJavaType();
    }

    @Override
    public Nullability getNullability() {
        return Nullability.NON_NULL;
    }

    private void assertValidName(String name) {
        if (!contains0(name)) {
            throw new NoSuchElementException("Column name '" + name + "' does not exist in " + this.nameSet);
        }
    }

    private boolean contains0(String name) {
        return nameSet.contains(name);
    }

    private <T> T get0(Class<?> type) {
        return codecs.decodeLastInsertId(lastInsertId, type);
    }

    private static void assertValidIndex(int index) {
        if (index != 0) {
            throw new ArrayIndexOutOfBoundsException("Index: " + index + ", total: 1");
        }
    }
}
