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
import io.github.mirromutth.r2dbc.mysql.config.ConnectProperties;
import io.github.mirromutth.r2dbc.mysql.message.frontend.PingMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.SimpleQueryMessage;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNotEmpty;

/**
 * An implementation of {@link Connection} for connecting to the MySQL database.
 */
public final class MySqlConnection implements Connection {

    private final Client client;

    private final AtomicBoolean autoCommit = new AtomicBoolean(true);

    MySqlConnection(Client client, ConnectProperties properties) {
        requireNonNull(client, "client must not be null");
        requireNonNull(properties, "properties must not be null");

        this.client = client;
    }

    @Override
    public Mono<Void> beginTransaction() {
        return Mono.defer(() -> {

            Mono<Void> prepare = Mono.empty();
            if (this.autoCommit.get()) {
                prepare = executeVoid("SET autocommit=0").doOnSuccess(ignore -> this.autoCommit.set(false));
            }

            return prepare.then(executeVoid("START TRANSACTION"));
        });
    }

    public Mono<Void> ping() {
        return this.client.exchange(PingMessage.getInstance()).then();
    }

    @Override
    public Mono<Void> close() {
        return this.client.close();
    }

    @Override
    public Mono<Void> commitTransaction() {
        return executeVoid("COMMIT");
    }

    @Override
    public MySqlBatch createBatch() {
        // TODO: implement this method
        return new MySqlBatch();
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        assertValidSavepointName(name);
        return executeVoid(String.format("SAVEPOINT `%s`", name));
    }

    @Override
    public MySqlStatement createStatement(String sql) {
        requireNonNull(sql, "sql must not be null");
        // TODO: implement this method
        return new SimpleQueryMySqlStatement(this.client, sql);
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        assertValidSavepointName(name);
        return executeVoid(String.format("RELEASE SAVEPOINT `%s`", name));
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return executeVoid("ROLLBACK");
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        assertValidSavepointName(name);
        return executeVoid(String.format("ROLLBACK TO SAVEPOINT `%s`", name));
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        requireNonNull(isolationLevel, "isolationLevel must not be null");
        return executeVoid(String.format("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql()));
    }

    private Mono<Void> executeVoid(String sql) {
        return Flux.defer(() -> this.client.exchange(new SimpleQueryMessage(sql)))
            .then();
    }

    private static void assertValidSavepointName(String name) {
        requireNotEmpty(name, "Savepoint name must not be empty");

        if (name.indexOf('`') != -1) {
            throw new IllegalArgumentException("Savepoint name must not contain backticks");
        }
    }
}
