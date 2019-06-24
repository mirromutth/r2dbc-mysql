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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link Query} for prepared statement.
 */
final class PrepareQuery implements Query {

    private final String sql;

    private final Map<String, int[]> nameKeyedIndex;

    private final int parameters;

    PrepareQuery(String sql, Map<String, int[]> nameKeyedIndex, int parameters) {
        this.sql = sql;
        this.nameKeyedIndex = nameKeyedIndex;
        this.parameters = parameters;
    }

    @Override
    public String getSql() {
        return sql;
    }

    int[] getIndexes(String identifier) {
        int[] index = nameKeyedIndex.get(identifier);

        if (index == null) {
            throw new IllegalArgumentException(String.format("No such parameter with identifier '%s'", identifier));
        }

        return index;
    }

    Set<String> getParameterNames() {
        return nameKeyedIndex.keySet();
    }

    int getParameters() {
        return parameters;
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
        StringBuilder builder = new StringBuilder(64 + (nameKeyedIndex.size() << 2))
            .append("PrepareQuery{sql=REDACTED, parameters=")
            .append(parameters)
            .append(", nameKeyedIndex=");

        Iterator<Map.Entry<String, int[]>> iter = nameKeyedIndex.entrySet().iterator();

        if (!iter.hasNext()) {
            return builder.append("{}") // Map start and end literal
                .append('}') // Object end literal
                .toString();
        }

        Map.Entry<String, int[]> entry = iter.next();
        builder.append('{') // Map start literal
            .append(entry.getKey())
            .append('=')
            .append(Arrays.toString(entry.getValue()));

        while (iter.hasNext()) {
            entry = iter.next();
            builder.append(',')
                .append(entry.getKey())
                .append('=')
                .append(Arrays.toString(entry.getValue()));
        }

        return builder.append('}') // Map end literal
            .append('}') // Object literal
            .toString();
    }
}
