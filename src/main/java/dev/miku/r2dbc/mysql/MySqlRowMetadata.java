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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import dev.miku.r2dbc.mysql.util.InternalArrays;
import io.r2dbc.spi.RowMetadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link RowMetadata} for MySQL database text/binary results.
 *
 * @see MySqlNames column name searching rules.
 */
final class MySqlRowMetadata implements RowMetadata {

    private static final Comparator<MySqlColumnMetadata> NAME_COMPARATOR = (left, right) ->
        MySqlNames.compare(left.getName(), right.getName());

    private final MySqlColumnMetadata[] idSorted;

    private final MySqlColumnMetadata[] nameSorted;

    /**
     * Copied column names from {@link #nameSorted}.
     */
    private final String[] names;

    private final ColumnNameSet nameSet;

    private MySqlRowMetadata(MySqlColumnMetadata[] idSorted) {
        int size = idSorted.length;

        if (size <= 0) {
            throw new IllegalArgumentException("least 1 column metadata");
        }

        MySqlColumnMetadata[] nameSorted = new MySqlColumnMetadata[size];
        System.arraycopy(idSorted, 0, nameSorted, 0, size);
        Arrays.sort(nameSorted, NAME_COMPARATOR);

        this.idSorted = idSorted;
        this.nameSorted = nameSorted;
        this.names = getNames(nameSorted);
        this.nameSet = new ColumnNameSet(this.names);
    }

    @Override
    public MySqlColumnMetadata getColumnMetadata(int index) {
        if (index < 0 || index >= idSorted.length) {
            throw new ArrayIndexOutOfBoundsException(String.format("column index %d is invalid, total %d", index, idSorted.length));
        }

        return idSorted[index];
    }

    @Override
    public MySqlColumnMetadata getColumnMetadata(String name) {
        requireNonNull(name, "name must not be null");

        int index = MySqlNames.nameSearch(this.names, name);

        if (index < 0) {
            throw new NoSuchElementException(String.format("column name '%s' does not exist in %s", name, Arrays.toString(this.names)));
        }

        return nameSorted[index];
    }

    @Override
    public List<MySqlColumnMetadata> getColumnMetadatas() {
        switch (idSorted.length) {
            case 0:
                return Collections.emptyList();
            case 1:
                return Collections.singletonList(idSorted[0]);
            default:
                return InternalArrays.asReadOnlyList(idSorted);
        }
    }

    @Override
    public Set<String> getColumnNames() {
        return nameSet;
    }

    @Override
    public String toString() {
        return String.format("MySqlRowMetadata{metadata=%s, sortedNames=%s}", Arrays.toString(idSorted), nameSet);
    }

    MySqlColumnMetadata[] unwrap() {
        return idSorted;
    }

    static MySqlRowMetadata create(DefinitionMetadataMessage[] columns) {
        int size = columns.length;
        MySqlColumnMetadata[] metadata = new MySqlColumnMetadata[size];

        for (int i = 0; i < size; ++i) {
            metadata[i] = MySqlColumnMetadata.create(i, columns[i]);
        }

        return new MySqlRowMetadata(metadata);
    }

    private static String[] getNames(MySqlColumnMetadata[] metadata) {
        int size = metadata.length;
        String[] names = new String[size];

        for (int i = 0; i < size; ++i) {
            names[i] = metadata[i].getName();
        }

        return names;
    }
}
