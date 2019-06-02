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

import io.github.mirromutth.r2dbc.mysql.internal.LazyLoad;
import io.github.mirromutth.r2dbc.mysql.message.server.ColumnMetadataMessage;
import io.r2dbc.spi.RowMetadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link RowMetadata} for MySQL database.
 */
final class MySqlRowMetadata implements RowMetadata {

    private final MySqlColumnMetadata[] columns;

    private final Map<String, MySqlColumnMetadata> nameKeyedColumns;

    private final LazyLoad<List<MySqlColumnMetadata>> immutableColumns;

    private final LazyLoad<Set<String>> immutableColumnNames;

    private MySqlRowMetadata(MySqlColumnMetadata[] columns) {
        this.columns = requireNonNull(columns, "columns must not be null");
        this.nameKeyedColumns = convertColumnMap(columns);
        this.immutableColumns = LazyLoad.of(() -> {
            switch (this.columns.length) {
                case 0:
                    return Collections.emptyList();
                case 1:
                    return Collections.singletonList(this.columns[0]);
                default:
                    return Collections.unmodifiableList(Arrays.asList(this.columns));
            }
        });
        this.immutableColumnNames = LazyLoad.of(() -> {
            switch (this.columns.length) {
                case 0:
                    return Collections.emptySet();
                case 1:
                    return Collections.singleton(this.columns[0].getName());
                default:
                    return Collections.unmodifiableSet(columnNames(this.columns));
            }
        });
    }

    @Override
    public MySqlColumnMetadata getColumnMetadata(Object identifier) {
        requireNonNull(identifier, "identifier must not be null");

        if (identifier instanceof Integer) {
            return columns[(Integer) identifier];
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
        return immutableColumns.get();
    }

    @Override
    public Set<String> getColumnNames() {
        return immutableColumnNames.get();
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

        return Arrays.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(columns);
    }

    @Override
    public String toString() {
        return "MySqlRowMetadata{" +
            "columns=" + Arrays.toString(columns) +
            '}';
    }

    static MySqlRowMetadata create(ColumnMetadataMessage[] columns) {
        int size = columns.length;
        MySqlColumnMetadata[] metadata = new MySqlColumnMetadata[size];

        for (int i = 0; i < size; ++i) {
            metadata[i] = MySqlColumnMetadata.create(i, columns[i]);
        }

        return new MySqlRowMetadata(metadata);
    }

    private static Set<String> columnNames(MySqlColumnMetadata[] columns) {
        // ceil(size / 0.75) = ceil(size / 3 * 4) = ceil(size / 3) * 4 = floor((size + 3 - 1) / 3) * 4
        Set<String> names = new LinkedHashSet<>(((columns.length + 2) / 3) * 4, 0.75f);

        for (MySqlColumnMetadata column : columns) {
            names.add(column.getName());
        }

        return names;
    }

    private static Map<String, MySqlColumnMetadata> convertColumnMap(MySqlColumnMetadata[] columns) {
        int size = columns.length;

        switch (size) {
            case 0:
                return Collections.emptyMap();
            case 1:
                MySqlColumnMetadata first = columns[0];
                return Collections.singletonMap(first.getName(), first);
            default:
                // ceil(size / 0.75) = ceil(size / 3 * 4) = ceil(size / 3) * 4 = floor((size + 3 - 1) / 3) * 4
                Map<String, MySqlColumnMetadata> result = new HashMap<>(((size + 2) / 3) * 4, 0.75f);
                for (MySqlColumnMetadata column : columns) {
                    result.put(column.getName(), column);
                }

                return result;
        }
    }
}
