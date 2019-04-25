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
import io.github.mirromutth.r2dbc.mysql.message.frontend.PingMessage;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import reactor.core.publisher.Mono;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Connection} for connecting to the MySQL database.
 */
final class MySqlConnection implements Connection {

    private final Client client;

    private MySqlConnection(Client client) {
        this.client = requireNonNull(client, "client must not be null");
    }

    @Override
    public Mono<Void> beginTransaction() {
        // TODO: implement this method
        return Mono.empty();
    }

    public Mono<Void> ping() {
        return client.exchange(PingMessage.getInstance()).then();
    }

    @Override
    public Mono<Void> close() {
        return client.close();
    }

    @Override
    public Mono<Void> commitTransaction() {
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public MySqlBatch createBatch() {
        // TODO: implement this method
        return new MySqlBatch();
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        requireNonNull(name, "name must not be null");
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public MySqlStatement createStatement(String sql) {
        return new SimpleQueryMySqlStatement(client, requireNonNull(sql, "sql must not be null"));
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        requireNonNull(name, "name must not be null");
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        requireNonNull(name, "name must not be null");
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        requireNonNull(isolationLevel, "isolationLevel must not be null");
        // TODO: implement this method
        return Mono.empty();
    }
}
