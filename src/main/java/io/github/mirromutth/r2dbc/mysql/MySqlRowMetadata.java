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

import io.r2dbc.spi.RowMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link RowMetadata} for MySQL database.
 */
final class MySqlRowMetadata implements RowMetadata {

    private final List<MySqlColumnMetadata> columns;

    private final Map<String, MySqlColumnMetadata> nameKeyedColumns;

    private final List<MySqlColumnMetadata> immutableColumns;

    MySqlRowMetadata(List<MySqlColumnMetadata> columns) {
        List<MySqlColumnMetadata> wrappedColumns = wrapColumns(requireNonNull(columns, "columns must not be null"));

        this.columns = wrappedColumns;
        this.nameKeyedColumns = convertColumnMap(wrappedColumns);
        this.immutableColumns = readOnlyColumns(wrappedColumns);
    }

    @Override
    public MySqlColumnMetadata getColumnMetadata(Object identifier) {
        if (requireNonNull(identifier, "identifier must not be null") instanceof Integer) {
            int index = (Integer) identifier;

            if (index >= columns.size() || index < 0) {
                // List will throw IndexOutOfBoundsException, it maybe NOT ArrayIndexOutOfBoundsException,
                // should throw ArrayIndexOutOfBoundsException in this method (for r2dbc SPI).
                throw new ArrayIndexOutOfBoundsException(index);
            }

            return columns.get(index);
        } else if (identifier instanceof String) {
            MySqlColumnMetadata column = nameKeyedColumns.get(identifier);

            if (column == null) {
                throw new NoSuchElementException("Column name '" + identifier + "' does not exist in column names " + this.nameKeyedColumns.keySet());
            }

            return column;
        }

        throw new IllegalArgumentException("Identifier '" + identifier + "' is not a valid identifier. Should either be an Integer index or a String column name.");
    }

    @Override
    public List<MySqlColumnMetadata> getColumnMetadatas() {
        return immutableColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlRowMetadata)) {
            return false;
        }

        MySqlRowMetadata that = (MySqlRowMetadata) o;

        return columns.equals(that.columns);

    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    public String toString() {
        return "MySqlRowMetadata{" +
            "columns=" + columns +
            '}';
    }

    private static List<MySqlColumnMetadata> wrapColumns(List<MySqlColumnMetadata> columns) {
        switch (columns.size()) {
            case 0:
                return Collections.emptyList();
            case 1:
                return Collections.singletonList(columns.get(0));
            default:
                return columns;
        }
    }

    private static List<MySqlColumnMetadata> readOnlyColumns(List<MySqlColumnMetadata> wrappedColumns) {
        if (wrappedColumns.size() < 2) {
            // it is EmptyList or SingletonList, also read only
            return wrappedColumns;
        }

        return Collections.unmodifiableList(wrappedColumns);
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private static Map<String, MySqlColumnMetadata> convertColumnMap(List<MySqlColumnMetadata> columns) {
        int size = columns.size();

        switch (size) {
            case 0:
                return Collections.emptyMap();
            case 1:
                MySqlColumnMetadata first = columns.get(0);
                return Collections.singletonMap(first.getName(), first);
            default:
                // ceil(size / 0.75) = ceil(size / 3 * 4) = ceil(size / 3) * 4 = floor((size + 3 - 1) / 3) * 4
                Map<String, MySqlColumnMetadata> result = new HashMap<>(((size + 2) / 3) * 4, 0.75f);

                // Use the old-style for loop, see: https://github.com/netty/netty/issues/2642
                for (int i = 0; i < size; ++i) {
                    MySqlColumnMetadata column = columns.get(i);
                    result.put(column.getName(), column);
                }

                return result;
        }
    }
}
