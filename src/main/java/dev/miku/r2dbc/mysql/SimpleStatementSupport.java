/*
 * Copyright 2018-2020 the original author or authors.
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

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.util.ConnectionContext;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Base class of {@link MySqlStatement} considers the simple statement that has no parameter.
 */
abstract class SimpleStatementSupport extends MySqlStatementSupport {

    protected final Client client;

    protected final Codecs codecs;

    protected final ConnectionContext context;

    protected final String sql;

    SimpleStatementSupport(Client client, Codecs codecs, ConnectionContext context, String sql) {
        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.context = requireNonNull(context, "context must not be null");
        this.sql = requireNonNull(sql, "sql must not be null");
    }

    @Override
    public final MySqlStatement add() {
        return this;
    }

    @Override
    public final MySqlStatement bind(int index, Object value) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple statement");
    }

    @Override
    public final MySqlStatement bind(String name, Object value) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple statement");
    }

    @Override
    public final MySqlStatement bindNull(int index, Class<?> type) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple statement");
    }

    @Override
    public final MySqlStatement bindNull(String name, Class<?> type) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple statement");
    }
}
