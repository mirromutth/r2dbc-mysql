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

import dev.miku.r2dbc.mysql.message.ParameterValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A structure of prepare statement and simple statement.
 */
final class Query {

    private final String sql;

    private final Map<String, Object> nameKeyedIndex;

    private final int parameters;

    private Query(String sql, Map<String, Object> nameKeyedIndex, int parameters) {
        this.sql = sql;
        this.nameKeyedIndex = nameKeyedIndex;
        this.parameters = parameters;
    }

    String getSql() {
        return sql;
    }

    Object getIndexes(String identifier) {
        Object index = nameKeyedIndex.get(identifier);

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

    boolean isPrepared() {
        return parameters > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Query)) {
            return false;
        }

        Query that = (Query) o;

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
        if (!isPrepared()) {
            return "Query{sql=REDACTED}";
        }

        return String.format("Query{sql=REDACTED, parameters=%d, nameKeyedIndex=%s", parameters, nameKeyedIndex);
    }

    /**
     * Parse parameter names of a parametrized SQL, and remove parameter names for parsed SQL which will be
     * send to MySQL server directly. The relationship between parameter names and parameter indexes will
     * be recorded by {@code nameKeyedParams}.
     * <p>
     * All parameters will be counted by {@code paramCount} even it has no name or has the same name of
     * other parameter.
     * <p>
     * For example:
     * {@code SELECT * FROM `test` WHERE (`username` = ?name OR `nickname` = ?name) AND `group` = ?} will
     * parse to {@code SELECT * FROM `test` WHERE `username` = ? OR `nickname` = ? AND `group` = ?}, and
     * mapped {@literal name} to {@literal 0} and {@literal 1}, {@code paramCount} will be {@literal 3}.
     *
     * @param sql    the statement want to parse.
     * @return parsed {@link Query}
     */
    static Query parse(String sql) {
        int offset = findParamMark(sql, 0);

        if (offset < 0) {
            return new Query(sql, Collections.emptyMap(), 0);
        }

        Map<String, Object> nameKeyedParams = new HashMap<>();
        // If sql does not contains named-parameter, sqlBuilder will always be null.
        StringBuilder sqlBuilder = null;
        // The last parameter end index of sql.
        int lastEnd = 0;
        String anyName = null;
        int length = sql.length();
        int paramCount = 0;

        while (offset >= 0 && offset < length) {
            ++paramCount;
            ++offset;

            if (offset < length) {
                char now = sql.charAt(offset);

                // Java style parameter name follow the '?'.
                if (Character.isJavaIdentifierStart(now)) {
                    int start = offset++;

                    while (offset < length) {
                        if (!Character.isJavaIdentifierPart(sql.charAt(offset))) {
                            break;
                        }

                        ++offset;
                    }

                    if (sqlBuilder == null) {
                        sqlBuilder = new StringBuilder(sql.length() - offset + start);
                    }

                    sqlBuilder.append(sql, lastEnd, start);
                    lastEnd = offset;

                    String name = sql.substring(start, offset);
                    int paramIndex = paramCount - 1;
                    Object value = nameKeyedParams.get(name);

                    anyName = name;

                    if (value == null) {
                        nameKeyedParams.put(name, paramIndex);
                    } else if (value instanceof Integer) {
                        nameKeyedParams.put(name, new Indexes((Integer) value, paramIndex));
                    } else {
                        ((Indexes) value).push(paramIndex);
                    }
                }
            } // offset is length or end of a parameter.

            if (offset < length) {
                offset = findParamMark(sql, offset);
            }
        }

        String parsedSql;

        if (sqlBuilder == null) {
            parsedSql = sql;
        } else if (lastEnd < length) {
            // Contains more plain sql.
            parsedSql = sqlBuilder.append(sql, lastEnd, length).toString();
        } else {
            parsedSql = sqlBuilder.toString();
        }

        switch (nameKeyedParams.size()) {
            case 0:
                return new Query(parsedSql, Collections.emptyMap(), paramCount);
            case 1:
                return new Query(parsedSql, Collections.singletonMap(anyName, nameKeyedParams.get(anyName)), paramCount);
            default:
                return new Query(parsedSql, nameKeyedParams, paramCount);
        }
    }

    /**
     * Locates the first occurrence of {@literal ?} return true in {@code sql} starting at {@code offset}.
     * <p>
     * The SQL string may contain:
     *
     * <ul>
     * <li>Literals, enclosed in single quotes ({@literal '}) </li>
     * <li>Literals, enclosed in double quotes ({@literal "}) </li>
     * <li>Literals, enclosed in backtick quotes ({@literal `}) </li>
     * <li>Escaped escapes or literal delimiters (i.e. {@literal ''}, {@literal ""} or {@literal ``)</li>
     * <li>Single-line comments beginning with {@literal --}</li>
     * <li>Multi-line comments beginning enclosed</li>
     * </ul>
     *
     * @param sql    the SQL string to search in.
     * @param offset the offset to start searching.
     * @return the offset or a negative integer if not found.
     */
    private static int findParamMark(CharSequence sql, int offset) {
        int length = sql.length();
        char ch;

        while (offset < length && offset >= 0) {
            ch = sql.charAt(offset++);
            switch (ch) {
                case '/':
                    if (offset == length) {
                        break;
                    }

                    if (sql.charAt(offset) == '*') {
                        // Consume if '/* ... */' comment.
                        while (++offset < length) {
                            if (sql.charAt(offset) == '*' && offset + 1 < length && sql.charAt(offset + 1) == '/') {
                                // If end of comment.
                                offset += 2;
                                break;
                            }
                        }
                        break;
                    }

                    break;
                case '-':
                    if (offset == length) {
                        break;
                    }

                    if (sql.charAt(offset) == '-') {
                        // Consume if '-- ... \n' comment.
                        while (++offset < length) {
                            char now = sql.charAt(offset);
                            if (now == '\n' || now == '\r') {
                                // If end of comment
                                offset++;
                                break;
                            }
                        }
                        break;
                    }

                    break;
                case '`':
                case '\'':
                case '"':
                    // Quote cases, should find same quote
                    while (offset < length) {
                        if (sql.charAt(offset++) == ch) {
                            if (length == offset || sql.charAt(offset) != ch) {
                                break;
                            }

                            ++offset;
                        }
                    }

                    break;
                default:
                    if (ch == '?') {
                        return offset - 1;
                    }

                    break;
            }
        }

        return -1;
    }

    static final class Indexes {

        private static final int INIT_CAPACITY = 8;

        private int size = 2;

        private int[] data;

        private Indexes(int first, int second) {
            int[] data = new int[INIT_CAPACITY];

            data[0] = first;
            data[1] = second;

            this.data = data;
        }

        private void push(int another) {
            int i = size++;

            if (i >= data.length) {
                int[] newData = new int[data.length << 1];
                System.arraycopy(data, 0, newData, 0, data.length);
                data = newData;
            }

            data[i] = another;
        }

        void bind(Binding binding, ParameterValue value) {
            for (int i = 0; i < size; ++i) {
                binding.add(data[i], value);
            }
        }

        int[] toIntArray() {
            return Arrays.copyOf(data, size);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Indexes)) {
                return false;
            }

            Indexes indexes = (Indexes) o;

            if (size != indexes.size) {
                return false;
            }

            for (int i = 0; i < size; ++i) {
                if (data[i] != indexes.data[i]) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = 1;

            for (int i = 0; i < size; ++i) {
                result = 31 * result + data[i];
            }

            return result;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder()
                .append('[')
                .append(data[0]);

            for (int i = 1; i < size; ++i) {
                builder.append(", ")
                    .append(data[i]);
            }

            return builder.append(']').toString();
        }
    }
}
