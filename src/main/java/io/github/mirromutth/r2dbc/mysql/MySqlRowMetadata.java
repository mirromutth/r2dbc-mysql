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
import io.github.mirromutth.r2dbc.mysql.message.server.DefinitionMetadataMessage;
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

    private final MySqlDefinitionMetadata[] metadata;

    private final Map<String, MySqlDefinitionMetadata> nameKeyedMetadata;

    private final LazyLoad<List<MySqlDefinitionMetadata>> immutableMetadata;

    private final LazyLoad<Set<String>> immutableMetadataNames;

    private MySqlRowMetadata(MySqlDefinitionMetadata[] metadata) {
        this.metadata = requireNonNull(metadata, "metadata must not be null");
        this.nameKeyedMetadata = convertColumnMap(metadata);
        this.immutableMetadata = LazyLoad.of(() -> {
            switch (this.metadata.length) {
                case 0:
                    return Collections.emptyList();
                case 1:
                    return Collections.singletonList(this.metadata[0]);
                default:
                    return Collections.unmodifiableList(Arrays.asList(this.metadata));
            }
        });
        this.immutableMetadataNames = LazyLoad.of(() -> {
            switch (this.metadata.length) {
                case 0:
                    return Collections.emptySet();
                case 1:
                    return Collections.singleton(this.metadata[0].getName());
                default:
                    return Collections.unmodifiableSet(columnNames(this.metadata));
            }
        });
    }

    @Override
    public MySqlDefinitionMetadata getColumnMetadata(Object identifier) {
        requireNonNull(identifier, "identifier must not be null");

        if (identifier instanceof Integer) {
            return metadata[(Integer) identifier];
        } else if (identifier instanceof String) {
            MySqlDefinitionMetadata column = nameKeyedMetadata.get(identifier);

            if (column == null) {
                throw new NoSuchElementException(String.format("Column name '%s' does not exist in column names %s", identifier, this.nameKeyedMetadata.keySet()));
            }

            return column;
        }

        throw new IllegalArgumentException("identifier should either be an Integer index or a String column name.");
    }

    @Override
    public List<MySqlDefinitionMetadata> getColumnMetadatas() {
        return immutableMetadata.get();
    }

    @Override
    public Set<String> getColumnNames() {
        return immutableMetadataNames.get();
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

        return Arrays.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(metadata);
    }

    @Override
    public String toString() {
        return "MySqlRowMetadata{" +
            "metadata=" + Arrays.toString(metadata) +
            '}';
    }

    static MySqlRowMetadata create(DefinitionMetadataMessage[] columns) {
        int size = columns.length;
        MySqlDefinitionMetadata[] metadata = new MySqlDefinitionMetadata[size];

        for (int i = 0; i < size; ++i) {
            metadata[i] = MySqlDefinitionMetadata.create(i, columns[i]);
        }

        return new MySqlRowMetadata(metadata);
    }

    private static Set<String> columnNames(MySqlDefinitionMetadata[] columns) {
        // ceil(size / 0.75) = ceil((size * 4) / 3) = floor((size * 4 + 3 - 1) / 3)
        Set<String> names = new LinkedHashSet<>(((columns.length << 2) + 2) / 3, 0.75f);

        for (MySqlDefinitionMetadata column : columns) {
            names.add(column.getName());
        }

        return names;
    }

    private static Map<String, MySqlDefinitionMetadata> convertColumnMap(MySqlDefinitionMetadata[] columns) {
        int size = columns.length;

        switch (size) {
            case 0:
                return Collections.emptyMap();
            case 1:
                MySqlDefinitionMetadata first = columns[0];
                return Collections.singletonMap(first.getName(), first);
            default:
                // ceil(size / 0.75) = ceil((size * 4) / 3) = floor((size * 4 + 3 - 1) / 3)
                Map<String, MySqlDefinitionMetadata> result = new HashMap<>(((size << 2) + 2) / 3, 0.75f);
                for (MySqlDefinitionMetadata column : columns) {
                    result.put(column.getName(), column);
                }

                return result;
        }
    }
}
