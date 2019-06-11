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

import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireValidName;

/**
 * Base class considers {@link #returnGeneratedValues(String...)} for {@link MySqlStatement} implementations.
 */
abstract class MySqlStatementSupport implements MySqlStatement {

    @Nullable
    String generatedKeyName = null;

    @Override
    public final MySqlStatementSupport returnGeneratedValues(String... columns) {
        requireNonNull(columns, "columns must not be null");

        if (columns.length == 1) {
            this.generatedKeyName = requireValidName(columns[0], "id name must not be empty and not contain backticks");
        } else if (columns.length > 1) {
            throw new IllegalArgumentException("MySQL only supports single generated value");
        }

        return this;
    }
}
