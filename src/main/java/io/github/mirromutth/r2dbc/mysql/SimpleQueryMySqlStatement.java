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
import io.github.mirromutth.r2dbc.mysql.message.frontend.SimpleQueryMessage;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlStatement} representing the simple query that has no parameter.
 */
final class SimpleQueryMySqlStatement implements MySqlStatement {

    private final Client client;

    private final String sql;

    SimpleQueryMySqlStatement(Client client, String sql) {
        this.client = requireNonNull(client, "client must not be null");
        this.sql = requireNonNull(sql, "sql must not be null");
    }

    @Override
    public SimpleQueryMySqlStatement add() {
        return this;
    }

    @Override
    public SimpleQueryMySqlStatement bind(@Nullable Object identifier, @Nullable Object value) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple query statement");
    }

    @Override
    public SimpleQueryMySqlStatement bind(int index, @Nullable Object value) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple query statement");
    }

    @Override
    public SimpleQueryMySqlStatement bindNull(@Nullable Object identifier, @Nullable Class<?> type) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple query statement");
    }

    @Override
    public SimpleQueryMySqlStatement bindNull(int index, @Nullable Class<?> type) {
        throw new UnsupportedOperationException("Binding parameters is not supported for simple query statement");
    }

    @Override
    public Flux<MySqlResult> execute() {
//        return client.exchange(new SimpleQueryMessage(sql)).map(message -> {});
        throw new RuntimeException();
    }
}
