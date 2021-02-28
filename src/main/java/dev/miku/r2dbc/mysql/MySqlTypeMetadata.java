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

/**
 * A metadata descriptor considers MySQL types.
 */
public final class MySqlTypeMetadata {

    private final int id;

    private final ColumnDefinition definition;

    MySqlTypeMetadata(int id, ColumnDefinition definition) {
        this.id = id;
        this.definition = definition;
    }

    /**
     * Get the native type identifier.
     *
     * @return the native type identifier.
     */
    public int getId() {
        return id;
    }

    /**
     * Get the {@link ColumnDefinition} that potentially exposes more type differences.
     *
     * @return the column definitions.
     */
    public ColumnDefinition getDefinition() {
        return definition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlTypeMetadata)) {
            return false;
        }

        MySqlTypeMetadata that = (MySqlTypeMetadata) o;

        return id == that.id && definition.equals(that.definition);
    }

    @Override
    public int hashCode() {
        return 31 * id + definition.hashCode();
    }

    @Override
    public String toString() {
        return "MySqlTypeMetadata(" + id + ", " + definition + ')';
    }
}
