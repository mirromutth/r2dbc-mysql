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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for statement parse, parametrize and mapping. It is a sealed class, has
 * implementations {@link SimpleQuery}, {@link PrepareQuery} and {@link TextQuery}.
 */
abstract class Query {

    abstract int getParameters();

    abstract ParameterIndex getIndexes(String identifier);

    static Query parse(String sql, boolean prepare) {
        int offset = findParamMark(sql, 0);

        if (offset < 0) {
            return SimpleQuery.INSTANCE;
        }

        Map<String, ParameterIndex> nameKeyedParams = new HashMap<>();
        // An parameter name, used by singleton map, if SQL does not contains named-parameter, it will always be empty.
        String anyName = "";
        // The last named parameter end index of sql.
        int lastNamedEnd = 0;
        // The last parameter end index (whatever named or not) of sql.
        int lastParamEnd = 0;
        int length = sql.length();
        int paramCount = 0;
        // ONLY FOR PREPARE: Parsed SQL builder, if SQL does not contains named-parameter, it will always be null.
        StringBuilder sqlBuilder = null;
        // ONLY FOR TEXT: SQL parts split by parameters.
        List<String> sqlParts;

        if (prepare) {
            // Prepare parsing, no need collect parts of SQL.
            sqlParts = Collections.emptyList();
        } else {
            sqlParts = new ArrayList<>();
        }

        while (offset >= 0 && offset < length) {
            if (!prepare) {
                sqlParts.add(subStr(sql, lastParamEnd, offset));
            }
            ++paramCount;
            // Assuming it has no name, then change later if it has, see command #Change.
            lastParamEnd = ++offset;

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

                    if (prepare) {
                        if (sqlBuilder == null) {
                            sqlBuilder = new StringBuilder(sql.length() - offset + start);
                        }

                        sqlBuilder.append(sql, lastNamedEnd, start);
                    }

                    // #Change: last parameter and last named parameter end index.
                    lastParamEnd = lastNamedEnd = offset;

                    String name = subStr(sql, start, offset);
                    int paramIndex = paramCount - 1;
                    ParameterIndex value = nameKeyedParams.get(name);

                    anyName = name;

                    if (value == null) {
                        nameKeyedParams.put(name, new ParameterIndex(paramIndex));
                    } else {
                        value.push(paramIndex);
                    }
                }
            } // offset is length or end of a parameter.

            if (offset < length) {
                offset = findParamMark(sql, offset);
            }
        }

        if (prepare) {
            String parsedSql;

            if (sqlBuilder == null) {
                parsedSql = sql;
            } else if (lastNamedEnd < length) {
                // Contains more plain sql.
                parsedSql = sqlBuilder.append(sql, lastNamedEnd, length).toString();
            } else {
                parsedSql = sqlBuilder.toString();
            }

            return new PrepareQuery(parsedSql, wrap(nameKeyedParams, anyName), paramCount);
        } else {
            sqlParts.add(subStr(sql, lastParamEnd, length));
            return new TextQuery(wrap(nameKeyedParams, anyName), sqlParts);
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

    private static String subStr(String sql, int start, int end) {
        if (start == end) {
            return "";
        } else {
            return sql.substring(start, end);
        }
    }
}
