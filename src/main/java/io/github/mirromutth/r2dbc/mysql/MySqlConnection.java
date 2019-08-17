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
import io.github.mirromutth.r2dbc.mysql.constant.Capabilities;
import io.github.mirromutth.r2dbc.mysql.constant.ServerStatuses;
import io.github.mirromutth.r2dbc.mysql.internal.ConnectionContext;
import io.github.mirromutth.r2dbc.mysql.message.client.PingMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.CommandDoneMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireValidName;

/**
 * An implementation of {@link Connection} for connecting to the MySQL database.
 */
public final class MySqlConnection implements Connection {

    private static final Logger logger = LoggerFactory.getLogger(MySqlConnection.class);

    private static final Consumer<ServerMessage> SAFE_RELEASE = ReferenceCountUtil::safeRelease;

    private static final BiConsumer<ServerMessage, SynchronousSink<Boolean>> PING_HANDLER = (message, sink) -> {
        if (message instanceof ErrorMessage) {
            ErrorMessage msg = (ErrorMessage) message;

            logger.debug("Remote validate failed: [{}] [{}] {}", msg.getErrorCode(), msg.getSqlState(), msg.getErrorMessage());

            sink.next(false);
            sink.complete();
        } else if (message instanceof CommandDoneMessage && ((CommandDoneMessage) message).isDone()) {
            sink.next(true);
            sink.complete();
        } else {
            ReferenceCountUtil.safeRelease(message);
        }
    };

    private final Client client;

    private final Codecs codecs;

    private final boolean batchSupported;

    private final ConnectionContext context;

    /**
     * @param client must be logged-in
     * @param context capabilities must be initialized
     */
    MySqlConnection(Client client, ConnectionContext context) {
        this.client = requireNonNull(client, "client must not be null");
        this.context = requireNonNull(context, "context must not be null");
        this.codecs = Codecs.getInstance();
        this.batchSupported = (context.getCapabilities() & Capabilities.MULTI_STATEMENTS) != 0;

        if (this.batchSupported) {
            logger.debug("Batch is supported by server");
        } else {
            logger.warn("The MySQL server does not support batch executing, fallback to executing one-by-one");
        }
    }

    @Override
    public Mono<Void> beginTransaction() {
        return Mono.defer(() -> {
            if (isInTransaction()) {
                return Mono.empty();
            }

            if (!isAutoCommit()) {
                return executeVoid("START TRANSACTION");
            } else if (batchSupported) {
                return executeVoid("SET autocommit=0;START TRANSACTION");
            } else {
                return executeVoid("SET autocommit=0").then(executeVoid("START TRANSACTION"));
            }
        });
    }

    @Override
    public Mono<Void> close() {
        Mono<Void> closer = client.close();

        if (logger.isDebugEnabled()) {
            return closer.doOnSubscribe(s -> logger.debug("Connection closing"))
                .doOnSuccess(ignored -> logger.debug("Connection close succeed"));
        }

        return closer;
    }

    @Override
    public Mono<Void> commitTransaction() {
        return Mono.defer(() -> {
            if (!isInTransaction()) {
                return Mono.empty();
            }

            if (isAutoCommit()) {
                return executeVoid("COMMIT");
            } else if (batchSupported) {
                return executeVoid("COMMIT;SET autocommit=1");
            } else {
                return executeVoid("COMMIT").then(executeVoid("SET autocommit=1"));
            }
        });
    }

    @Override
    public MySqlBatch createBatch() {
        if (batchSupported) {
            return new MySqlBatchingBatch(client, codecs, context);
        } else {
            return new MySqlSyntheticBatch(client, codecs, context);
        }
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        requireValidName(name, "Savepoint name must not be empty and not contain backticks");

        return executeVoid(String.format("SAVEPOINT `%s`", name));
    }

    /**
     * {@inheritDoc}
     *
     * @param sql the SQL of the statement, should include only one-statement, otherwise stream terminate disordered.
     */
    @Override
    public MySqlStatement createStatement(String sql) {
        requireNonNull(sql, "sql must not be null");

        int index = PrepareQuery.indexOfParameter(sql);

        if (index < 0) {
            // No parameter mark, it must be simple query.
            logger.debug("Create a statement provided by simple query");
            return new SimpleMySqlStatement(client, codecs, context, sql);
        } else {
            // Find parameter mark, it should be prepare query.
            logger.debug("Create a statement provided by prepare query");
            return new ParametrizedMySqlStatement(client, codecs, context, PrepareQuery.parse(sql, index));
        }
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        requireValidName(name, "Savepoint name must not be empty and not contain backticks");

        return executeVoid(String.format("RELEASE SAVEPOINT `%s`", name));
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return Mono.defer(() -> {
            if (!isInTransaction()) {
                return Mono.empty();
            }

            if (isAutoCommit()) {
                return executeVoid("ROLLBACK");
            } else if (batchSupported) {
                return executeVoid("ROLLBACK;SET autocommit=1");
            } else {
                return executeVoid("ROLLBACK").then(executeVoid("SET autocommit=1"));
            }
        });
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        requireValidName(name, "Savepoint name must not be empty and not contain backticks");

        return executeVoid(String.format("ROLLBACK TO SAVEPOINT `%s`", name));
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        requireNonNull(isolationLevel, "isolationLevel must not be null");

        return executeVoid(String.format("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql()));
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {
        requireNonNull(depth, "depth must not be null");

        if (depth == ValidationDepth.LOCAL) {
            return Mono.fromSupplier(client::isConnected);
        }

        return Mono.defer(() -> {
            if (!client.isConnected()) {
                return Mono.just(false);
            }

            return client.exchange(PingMessage.getInstance())
                .handle(PING_HANDLER)
                .last()
                .onErrorResume(e -> {
                    // `last` maybe emit a NoSuchElementException, exchange maybe emit exception by Netty.
                    // But should NEVER emit any exception in this method, so logging exception and emit false.
                    logger.debug("Remote validate failed", e);
                    return Mono.just(false);
                });
        });
    }

    boolean isAutoCommit() {
        return (context.getServerStatuses() & ServerStatuses.AUTO_COMMIT) != 0;
    }

    boolean isInTransaction() {
        return (context.getServerStatuses() & ServerStatuses.IN_TRANSACTION) != 0;
    }

    private Mono<Void> executeVoid(String sql) {
        return SimpleQueryFlow.execute(client, sql).doOnNext(SAFE_RELEASE).then();
    }
}
