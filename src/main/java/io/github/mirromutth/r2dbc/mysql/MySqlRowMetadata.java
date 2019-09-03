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

import io.github.mirromutth.r2dbc.mysql.codec.FieldInformation;
import io.github.mirromutth.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import io.r2dbc.spi.RowMetadata;

import java.util.List;
import java.util.Set;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link RowMetadata} for MySQL database text/binary results.
 */
final class MySqlRowMetadata implements RowMetadata {

    private final CollatedColumnMetadata collated;

    private MySqlRowMetadata(MySqlColumnMetadata[] metadata) {
        this.collated = new CollatedColumnMetadata(metadata);
    }

    @Override
    public MySqlColumnMetadata getColumnMetadata(Object identifier) {
        requireNonNull(identifier, "identifier must not be null");

        if (identifier instanceof Integer) {
            return collated.getMetadata((Integer) identifier);
        } else if (identifier instanceof String) {
            return collated.getMetadata((String) identifier);
        }

        throw new IllegalArgumentException("identifier should either be an Integer index or a String column name.");
    }

    @Override
    public List<MySqlColumnMetadata> getColumnMetadatas() {
        return collated.allValues();
    }

    @Override
    public Set<String> getColumnNames() {
        return collated.nameSet();
    }

    @Override
    public String toString() {
        return String.format("MySqlRowMetadata{metadata=%s}", collated.toString());
    }

    MySqlColumnMetadata[] unwrap() {
        return collated.getIdSorted();
    }

    static MySqlRowMetadata create(DefinitionMetadataMessage[] columns) {
        int size = columns.length;
        MySqlColumnMetadata[] metadata = new MySqlColumnMetadata[size];

        for (int i = 0; i < size; ++i) {
            metadata[i] = MySqlColumnMetadata.create(i, columns[i]);
        }

        return new MySqlRowMetadata(metadata);
    }
}
