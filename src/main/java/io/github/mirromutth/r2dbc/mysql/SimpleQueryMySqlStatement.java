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

import io.github.mirromutth.r2dbc.mysql.client.Client;
import io.github.mirromutth.r2dbc.mysql.codec.Codecs;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import reactor.core.publisher.Mono;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlStatement} representing the simple query that has no parameter.
 */
final class SimpleQueryMySqlStatement extends MySqlStatementSupport {

    private final Client client;

    private final Codecs codecs;

    private final MySqlSession session;

    private final String sql;

    SimpleQueryMySqlStatement(Client client, Codecs codecs, MySqlSession session, String sql) {
        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.session = requireNonNull(session, "session must not be null");
        this.sql = requireNonNull(sql, "sql must not be null");
    }

    @Override
    public MySqlStatementSupport add() {
        return this;
    }

    @Override
    public MySqlStatementSupport bind(Object identifier, Object value) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple query statement");
    }

    @Override
    public MySqlStatementSupport bind(int index, Object value) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple query statement");
    }

    @Override
    public MySqlStatementSupport bindNull(Object identifier, Class<?> type) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple query statement");
    }

    @Override
    public MySqlStatementSupport bindNull(int index, Class<?> type) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple query statement");
    }

    @Override
    public Mono<MySqlResult> execute() {
        return Mono.fromSupplier(() -> new MySqlResult(codecs, session, generatedKeyName, SimpleQueryFlow.execute(client, sql)));
    }
}
