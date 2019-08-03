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

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Query} for batch executing statements.
 */
final class BatchQuery implements Query {

    private StringBuilder builder;

    void add(String sql) {
        requireNonNull(sql, "sql must not be null");

        int index = lastNonWhitespace(sql);

        if (sql.charAt(index) == ';') {
            // Skip last ';' and whitespaces that following last ';'.
            requireBuilder().append(sql, 0, index);
        } else {
            requireBuilder().append(sql);
        }
    }

    @Override
    public String getSql() {
        return builder.toString();
    }

    private StringBuilder requireBuilder() {
        if (builder == null) {
            return (builder = new StringBuilder());
        }

        return builder.append(';');
    }

    private static int lastNonWhitespace(String sql) {
        int size = sql.length();

        for (int i = size - 1; i >= 0; --i) {
            if (!Character.isWhitespace(sql.charAt(i))) {
                return i;
            }
        }

        return -1;
    }
}
