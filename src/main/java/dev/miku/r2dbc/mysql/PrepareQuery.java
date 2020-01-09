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

import java.util.Map;
import java.util.Set;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A structure of prepare statement and simple statement.
 */
final class PrepareQuery extends Query {

    private final String sql;

    private final Map<String, ParameterIndex> nameKeyedIndex;

    private final int parameters;

    PrepareQuery(String sql, Map<String, ParameterIndex> nameKeyedIndex, int parameters) {
        require(parameters > 0, "parameters must be greater than 0");
        requireNonNull(nameKeyedIndex, "named parameter map must not be null");

        this.sql = sql;
        this.nameKeyedIndex = nameKeyedIndex;
        this.parameters = parameters;
    }

    @Override
    int getParameters() {
        return parameters;
    }

    @Override
    ParameterIndex getIndexes(String identifier) {
        ParameterIndex index = nameKeyedIndex.get(identifier);

        if (index == null) {
            throw new IllegalArgumentException(String.format("No such parameter with identifier '%s'", identifier));
        }

        return index;
    }

    Set<String> getParameterNames() {
        return nameKeyedIndex.keySet();
    }

    String getSql() {
        return sql;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PrepareQuery)) {
            return false;
        }

        PrepareQuery that = (PrepareQuery) o;

        if (parameters != that.parameters) {
            return false;
        }
        if (!sql.equals(that.sql)) {
            return false;
        }
        return nameKeyedIndex.equals(that.nameKeyedIndex);
    }

    @Override
    public int hashCode() {
        int result = sql.hashCode();
        result = 31 * result + nameKeyedIndex.hashCode();
        result = 31 * result + parameters;
        return result;
    }

    @Override
    public String toString() {
        return String.format("PrepareQuery{sql=REDACTED, parameters=%d, nameKeyedIndex=%s", parameters, nameKeyedIndex);
    }
}
