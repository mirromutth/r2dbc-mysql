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
import io.github.mirromutth.r2dbc.mysql.converter.Converters;
import io.github.mirromutth.r2dbc.mysql.core.LazyLoad;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.json.MySqlJson;
import io.github.mirromutth.r2dbc.mysql.json.MySqlJsonFactory;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.PingMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.SimpleQueryMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNotEmpty;

/**
 * An implementation of {@link Connection} for connecting to the MySQL database.
 */
public final class MySqlConnection implements Connection {

    private final Client client;

    private final MySqlSession session;

    private final AtomicBoolean autoCommit = new AtomicBoolean(true);

    private volatile MySqlJsonFactory jsonFactory;

    private final LazyLoad<Converters> textConverters;

    MySqlConnection(Client client, MySqlSession session, MySqlJsonFactory jsonFactory) {
        this.client = requireNonNull(client, "client must not be null");
        this.session = requireNonNull(session, "session must not be null");
        this.jsonFactory = requireNonNull(jsonFactory, "jsonFactory must not be null");
        this.textConverters = LazyLoad.of(() -> {
            MySqlJson mySqlJson = this.jsonFactory.build(session.getServerVersion());
            this.jsonFactory = null;
            return Converters.text(mySqlJson, this.session);
        });
    }

    @Override
    public Mono<Void> beginTransaction() {
        return Mono.defer(() -> {
            Mono<Void> prepare;

            if (this.autoCommit.get()) {
                prepare = executeVoid("SET autocommit=0").doOnSuccess(ignore -> this.autoCommit.set(false));
            } else {
                prepare = Mono.empty();
            }

            return prepare.then(executeVoid("START TRANSACTION"));
        });
    }

    public Mono<Void> ping() {
        return this.client.exchange(Mono.just(PingMessage.getInstance())).handle((message, sink) -> {
            if (message instanceof ErrorMessage) {
                sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
            } else if (message instanceof OkMessage) {
                sink.complete();
            } else {
                ReferenceCountUtil.release(message);
            }
        }).then();
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
        return new SimpleQueryMySqlStatement(client, textConverters.get(), sql);
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
        return this.client.exchange(Mono.fromSupplier(() -> new SimpleQueryMessage(sql)))
            .handle((message, sink) -> {
                if (message instanceof ErrorMessage) {
                    sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                } else if (message instanceof OkMessage) {
                    sink.complete();
                } else {
                    ReferenceCountUtil.release(message);
                }
            })
            .then();
    }

    private static void assertValidSavepointName(String name) {
        requireNotEmpty(name, "Savepoint name must not be empty");

        if (name.indexOf('`') != -1) {
            throw new IllegalArgumentException("Savepoint name must not contain backticks");
        }
    }
}
