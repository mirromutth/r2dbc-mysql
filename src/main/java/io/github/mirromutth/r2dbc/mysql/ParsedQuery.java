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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Parse parameter names of a parametrized SQL, and remove parameter names for parsed SQL which will be
 * send to MySQL server directly. The relationship between parameter names and parameter indexes will
 * be recorded by {@link #nameKeyedIndex}.
 * <p>
 * All parameters will be counted by {@link #parameters} even it has no name or has the same name of
 * other parameter.
 * <p>
 * For example:
 * {@code SELECT * FROM `test` WHERE (`username` = ?name OR `nickname` = ?name) AND `group` = ?} will
 * parse to {@code SELECT * FROM `test` WHERE `username` = ? OR `nickname` = ? AND `group` = ?}, and
 * mapped {@literal name} to {@literal 0} and {@literal 1}, {@link #parameters} is {@literal 3}.
 *
 * @see ParametrizedMySqlStatement
 */
final class ParsedQuery {

    private final String sql;

    private final Map<String, int[]> nameKeyedIndex;

    private final int parameters;

    private ParsedQuery(String sql, Map<String, int[]> nameKeyedIndex, int parameters) {
        this.sql = sql;
        this.nameKeyedIndex = nameKeyedIndex;
        this.parameters = parameters;
    }

    String getSql() {
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
        if (!(o instanceof ParsedQuery)) {
            return false;
        }

        ParsedQuery that = (ParsedQuery) o;

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
        return "ParsedQuery{" +
            "sql=REDACTED" +
            ", nameKeyedIndex=" + nameKeyedIndex +
            ", parameters=" + parameters +
            '}';
    }

    static boolean hasParameter(String sql) {
        requireNonNull(sql, "sql must not be null");

        return findChar('?', sql, 0) >= 0;
    }

    static boolean isOneStatement(String sql) {
        requireNonNull(sql, "sql must not be null");

        int index = findChar(';', sql, 0);

        if (index < 0) {
            return true;
        }

        int length = sql.length();

        for (int i = index + 1; i < length; ++i) {
            // Note: MySQL treats lots of character as whitespace, like ' ', '\t', '\r', '\n', '\f', '\u000B', etc.
            if (!Character.isWhitespace(sql.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    static ParsedQuery parse(String sql) {
        requireNonNull(sql, "sql must not be null");

        Map<String, List<Integer>> nameKeyedParams = new HashMap<>();
        SqlBuilder sqlBuilder = new SqlBuilder(sql);

        String anyName = null;

        int offset = 0;
        int length = sql.length();
        int paramCount = 0;

        while (offset < length) {
            offset = findChar('?', sql, offset);

            if (offset < 0) {
                break;
            }

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

                    String name = sqlBuilder.parameter(start, offset);
                    int paramIndex = paramCount - 1;

                    anyName = name;

                    if (nameKeyedParams.containsKey(name)) {
                        List<Integer> value = nameKeyedParams.get(name);
                        value.add(paramIndex);
                    } else {
                        List<Integer> value = new ArrayList<>();
                        value.add(paramIndex);
                        nameKeyedParams.put(name, value);
                    }
                }
            } // offset is length or end of a parameter.
        }

        String parsedSql = sqlBuilder.toString();
        int mapSize = nameKeyedParams.size();

        if (anyName == null || mapSize == 0) {
            return new ParsedQuery(parsedSql, Collections.emptyMap(), paramCount);
        }

        if (mapSize == 1) {
            return new ParsedQuery(parsedSql, Collections.singletonMap(anyName, convert(nameKeyedParams.get(anyName))), paramCount);
        }

        // ceil(size / 0.75) = ceil((size * 4) / 3) = floor((size * 4 + 3 - 1) / 3)
        Map<String, int[]> indexesMap = new HashMap<>(((mapSize << 2) + 2) / 3, 0.75f);

        for (Map.Entry<String, List<Integer>> entry : nameKeyedParams.entrySet()) {
            List<Integer> value = entry.getValue();

            if (value != null && !value.isEmpty()) {
                indexesMap.put(entry.getKey(), convert(value));
            }
        }

        return new ParsedQuery(parsedSql, indexesMap, paramCount);
    }

    /**
     * Locates the first occurrence of {@code needle} in {@code sql} starting at {@code offset}. The SQL string may contain:
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
     * @param needle the character to search for.
     * @param sql    the SQL string to search in.
     * @param offset the offset to start searching.
     * @return the offset or a negative integer if not found.
     */
    private static int findChar(char needle, CharSequence sql, int offset) {
        char character;
        int length = sql.length();

        while (offset < length && offset >= 0) {
            character = sql.charAt(offset++);
            switch (character) {
                case '/':
                    if (offset == length) {
                        break;
                    }

                    if (sql.charAt(offset) == '*') { // If '/* ... */' comment
                        while (++offset < length) { // consume comment
                            if (sql.charAt(offset) == '*' && offset + 1 < length && sql.charAt(offset + 1) == '/') { // If
                                // end of comment
                                offset += 2;
                                break;
                            }
                        }
                        break;
                    }

                    if (sql.charAt(offset) == '-') {
                        break;
                    }

                    if (needle == character) {
                        return offset - 1;
                    }

                    break;
                case '-':
                    if (sql.charAt(offset) == '-') { // If '-- ... \n' comment
                        while (++offset < length) { // consume comment
                            if (sql.charAt(offset) == '\n' || sql.charAt(offset) == '\r') { // If end of comment
                                offset++;
                                break;
                            }
                        }
                        break;
                    }

                    if (needle == character) {
                        return offset - 1;
                    }

                    break;
                default:
                    if (needle == character) {
                        return offset - 1;
                    }

                    break;
                case '`':
                case '\'':
                case '"':
                    // Quote cases, should find same quote
                    while (offset < length) {
                        if (sql.charAt(offset++) == character) {
                            if (length == offset || sql.charAt(offset) != character) {
                                break;
                            }

                            ++offset;
                        }
                    }

                    break;
            }
        }

        return -1;
    }

    private static int[] convert(List<Integer> indexes) {
        int size = indexes.size();
        int[] result = new int[size];

        for (int i = 0; i < size; ++i) {
            result[i] = indexes.get(i);
        }

        return result;
    }

    private static final class SqlBuilder {

        private final String sql;

        private int lastEnd = 0;

        private StringBuilder builder;

        private SqlBuilder(String sql) {
            this.sql = sql;
        }

        private String parameter(int start, int end) {
            getBuilder().append(sql, lastEnd, start);
            lastEnd = end;
            return sql.substring(start, end);
        }

        private StringBuilder getBuilder() {
            if (builder == null) {
                builder = new StringBuilder(sql.length());
            }

            return builder;
        }

        @Override
        public String toString() {
            if (builder == null) {
                return sql;
            }

            int length = sql.length();

            if (lastEnd < length) {
                return builder.append(sql, lastEnd, length).toString();
            }

            return builder.toString();
        }
    }
}
