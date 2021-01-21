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

import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A data class contains parsed statement and indexes mapping.
 */
public final class Query {

    private final String sql;

    private final Map<String, ParameterIndex> namedIndexes;

    private final List<Part> parts;

    private final int formattedSize;

    @Nullable
    private String formattedSql;

    private Query(String sql, Map<String, ParameterIndex> namedIndexes, List<Part> parts, int formattedSize) {
        this.sql = sql;
        this.namedIndexes = namedIndexes;
        this.parts = parts;
        this.formattedSize = formattedSize;
    }

    /**
     * Writes an index specified statement part to a {@link StringBuilder}.
     *
     * @param builder the {@link StringBuilder}
     * @param i       the index.
     */
    public void partTo(StringBuilder builder, int i) {
        Part part = parts.get(i);
        builder.append(sql, part.start, part.end);
    }

    /**
     * Get the length of formatted statement.
     *
     * @return the formatted size.
     */
    public int getFormattedSize() {
        return formattedSize;
    }

    /**
     * Get the number of parts.
     *
     * @return the number of parts.
     */
    public int getPartSize() {
        return parts.size();
    }

    boolean isSimple() {
        return parts.isEmpty();
    }

    int getParameters() {
        int size = parts.size();
        return size > 1 ? size - 1 : 0;
    }

    String getFormattedSql() {
        String formattedSql = this.formattedSql;

        if (formattedSql == null) {
            if (namedIndexes.isEmpty()) {
                this.formattedSql = formattedSql = sql;
            } else {
                Part part = parts.get(0);
                char[] result = new char[formattedSize];
                int size = parts.size();
                int length = 0;

                if (part.end > part.start) {
                    sql.getChars(part.start, part.end, result, 0);
                    length = part.end - part.start;
                }

                for (int i = 1; i < size; ++i) {
                    result[length++] = '?';
                    part = parts.get(i);

                    if (part.end > part.start) {
                        sql.getChars(part.start, part.end, result, length);
                        length += part.end - part.start;
                    }
                }

                this.formattedSql = formattedSql = new String(result);
            }
        }

        return formattedSql;
    }

    Map<String, ParameterIndex> getNamedIndexes() {
        return namedIndexes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Query query = (Query) o;

        return formattedSize == query.formattedSize && sql.equals(query.sql) &&
            namedIndexes.equals(query.namedIndexes) && parts.equals(query.parts);
    }

    @Override
    public int hashCode() {
        int result = sql.hashCode();
        result = 31 * result + namedIndexes.hashCode();
        result = 31 * result + parts.hashCode();
        return 31 * result + formattedSize;
    }

    @Override
    public String toString() {
        return "Query{sql='" + sql + "', namedIndexes=" + namedIndexes +
            ", parts=" + parts + ", formattedSize=" + formattedSize + '}';
    }

    /**
     * Parses an origin statement as a {@link Query}.
     *
     * @param sql the origin statement.
     * @return parsed {@link Query}.
     */
    public static Query parse(String sql) {
        int offset = findParamMark(sql, 0);

        if (offset < 0) {
            return new Query(sql, Collections.emptyMap(), Collections.emptyList(), sql.length());
        }

        Map<String, ParameterIndex> nameKeyedParams = new HashMap<>();
        // Used by singleton map, if SQL does not contains named-parameter, it will always be empty.
        String anyName = "";
        // The last parameter end index (whatever named or not) of sql.
        int lastParamEnd = 0;
        int length = sql.length();
        int paramCount = 0;
        int formattedSize = 0;
        // SQL parts split by parameters.
        List<Part> parts = new ArrayList<>();

        while (offset >= 0 && offset < length) {
            parts.add(Part.of(lastParamEnd, offset));
            formattedSize += offset - lastParamEnd + 1;
            ++paramCount;
            // Assuming it has no name, then change later if it has, see command #Change.
            lastParamEnd = ++offset;

            if (offset < length) {
                // Java style parameter name follow the '?'.
                if (Character.isJavaIdentifierStart(sql.charAt(offset))) {
                    int start = offset++;

                    while (offset < length && Character.isJavaIdentifierPart(sql.charAt(offset))) {
                        ++offset;
                    }

                    // #Change: last parameter and last named parameter end index.
                    lastParamEnd = offset;

                    String name = sql.substring(start, offset);
                    int paramIndex = paramCount - 1;
                    ParameterIndex value = nameKeyedParams.get(name);

                    anyName = name;

                    if (value == null) {
                        nameKeyedParams.put(name, new ParameterIndex(paramIndex));
                    } else {
                        value.push(paramIndex);
                    }

                    if (offset < length) {
                        offset = findParamMark(sql, offset);
                    }
                } else {
                    offset = findParamMark(sql, offset);
                }
            } // offset is length or end of a parameter.
        }

        parts.add(Part.of(lastParamEnd, length));
        formattedSize += length - lastParamEnd;

        return new Query(sql, wrap(nameKeyedParams, anyName), parts, formattedSize);
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
     * <li>Escaped escapes or literal delimiters (i.e. {@literal ''}, {@literal ""} or {@literal ``})</li>
     * <li>Single-line comments beginning with {@literal --}</li>
     * <li>Multi-line comments beginning enclosed</li>
     * </ul>
     *
     * @param sql   the SQL string to search in.
     * @param start the offset to start searching.
     * @return the offset or a negative integer if not found.
     */
    private static int findParamMark(CharSequence sql, int start) {
        int offset = start;
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
                            if (sql.charAt(offset) == '*' && offset + 1 < length &&
                                sql.charAt(offset + 1) == '/') {
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

    private static Map<String, ParameterIndex> wrap(Map<String, ParameterIndex> map, String anyKey) {
        switch (map.size()) {
            case 0:
                return Collections.emptyMap();
            case 1:
                return Collections.singletonMap(anyKey, map.get(anyKey));
            default:
                return map;
        }
    }

    private static final class Part {

        private static final Part EMPTY = new Part(0, 0);

        private final int start;

        private final int end;

        private Part(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Part part = (Part) o;

            return start == part.start && end == part.end;
        }

        @Override
        public int hashCode() {
            return Integer.reverse(start) ^ end;
        }

        @Override
        public String toString() {
            return start == end ? "()" : "(" + start + ", " + end + ')';
        }

        static Part of(int start, int end) {
            return start == end ? EMPTY : new Part(start, end);
        }
    }
}
