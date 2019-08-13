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
import io.github.mirromutth.r2dbc.mysql.message.server.CompleteMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireValidName;

/**
 * An implementation of {@link Connection} for connecting to the MySQL database.
 */
public final class MySqlConnection implements Connection {

    private static final Logger logger = LoggerFactory.getLogger(MySqlConnection.class);

    /**
     * If MySQL server version greater than or equal to {@literal 8.0.3}, or greater than
     * or equal to {@literal 5.7.20} and less than {@literal 8.0.0}, the column name of
     * current session isolation level will be {@literal @@transaction_isolation},
     * otherwise it is {@literal @@tx_isolation}.
     *
     * @see #create judge server version before get the isolation level.
     */
    private static final ServerVersion TRAN_LEVEL_8x = ServerVersion.create(8, 0, 3);

    private static final ServerVersion TRAN_LEVEL_5x = ServerVersion.create(5, 7, 20);

    private static final ServerVersion TX_LEVEL_8x = ServerVersion.create(8, 0, 0);

    private static final Predicate<ServerMessage> PING_DONE = message ->
        message instanceof ErrorMessage || (message instanceof CompleteMessage && ((CompleteMessage) message).isDone());

    /**
     * Convert result to isolation level which considered {@code null}.
     */
    private static final Function<MySqlResult, Publisher<IsolationLevel>> ISOLATION_LEVEL_HANDLER =
        r -> r.map((row, meta) -> convertIsolationLevel(row.get(0, String.class)));

    private static final Consumer<ServerMessage> SAFE_RELEASE = ReferenceCountUtil::safeRelease;

    private static final BiConsumer<ServerMessage, SynchronousSink<Boolean>> PING_HANDLER = (message, sink) -> {
        if (message instanceof ErrorMessage) {
            ErrorMessage msg = (ErrorMessage) message;
            logger.debug("Remote validate failed: [{}] [{}] {}", msg.getErrorCode(), msg.getSqlState(), msg.getErrorMessage());
            sink.next(false);
        } else if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
            sink.next(true);
        } else {
            ReferenceCountUtil.safeRelease(message);
        }
    };

    private final Client client;

    private final Codecs codecs;

    private final boolean batchSupported;

    private final ConnectionContext context;

    private final IsolationLevel sessionLevel;

    @Nullable
    private final QueryCache queryCache;

    @Nullable
    private final StatementCache statementCache;

    /**
     * Current isolation level inferred by past statements.
     * <p>
     * Inference rules:
     * <ol>
     * <li>In the beginning, it is also {@link #sessionLevel}.</li>
     * <li>After the user calls {@link #setTransactionIsolationLevel(IsolationLevel)}, it will change to the user-specified value.</li>
     * <li>After the end of a transaction (commit or rollback), it will recover to {@link #sessionLevel}.</li>
     * </ol>
     */
    private volatile IsolationLevel currentLevel;

    private MySqlConnection(
        Client client,
        ConnectionContext context,
        Codecs codecs,
        IsolationLevel sessionLevel,
        @Nullable QueryCache queryCache,
        @Nullable StatementCache statementCache
    ) {
        this.client = client;
        this.context = context;
        this.sessionLevel = sessionLevel;
        this.currentLevel = sessionLevel;
        this.codecs = codecs;
        this.queryCache = queryCache;
        this.statementCache = statementCache;
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

            Mono<Void> commit;

            if (isAutoCommit()) {
                commit = executeVoid("COMMIT");
            } else if (batchSupported) {
                commit = executeVoid("COMMIT;SET autocommit=1");
            } else {
                commit = executeVoid("COMMIT").then(executeVoid("SET autocommit=1"));
            }

            return recoverIsolationLevel(commit);
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

        String sql = String.format("SAVEPOINT `%s`", name);

        return Mono.defer(() -> {
            if (isInTransaction()) {
                return executeVoid(sql);
            }

            // See Example.savePointStartsTransaction, if connection does not in transaction, then starts transaction.
            if (batchSupported) {
                if (isAutoCommit()) {
                    return executeVoid("SET autocommit=0;START TRANSACTION;" + sql);
                } else {
                    return executeVoid("START TRANSACTION;" + sql);
                }
            } else {
                Mono<Void> start;

                if (isAutoCommit()) {
                    start = executeVoid("SET autocommit=0").then(executeVoid("START TRANSACTION"));
                } else {
                    start = executeVoid("START TRANSACTION");
                }

                return start.then(executeVoid(sql));
            }
        });
    }

    @Override
    public MySqlStatement createStatement(String sql) {
        requireNonNull(sql, "sql must not be null");

        Query query;

        if (queryCache == null) {
            query = Query.parse(sql);
        } else {
            query = queryCache.getOrParse(sql);
        }

        if (query.isPrepared()) {
            // Find parameter mark, it should be prepare query.
            logger.debug("Create a statement provided by prepare query");
            return new ParametrizedMySqlStatement(client, codecs, context, query, statementCache);
        } else {
            // No parameter mark, it must be simple query.
            logger.debug("Create a statement provided by simple query");
            return new SimpleMySqlStatement(client, codecs, context, sql);
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

            Mono<Void> rollback;

            if (isAutoCommit()) {
                rollback = executeVoid("ROLLBACK");
            } else if (batchSupported) {
                rollback = executeVoid("ROLLBACK;SET autocommit=1");
            } else {
                rollback = executeVoid("ROLLBACK").then(executeVoid("SET autocommit=1"));
            }

            return recoverIsolationLevel(rollback);
        });
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        requireValidName(name, "Savepoint name must not be empty and not contain backticks");

        return executeVoid(String.format("ROLLBACK TO SAVEPOINT `%s`", name));
    }

    /**
     * MySQL does not have any way to query the isolation level of the current transaction,
     * only inferred from past statements, so driver can not make sure the result is right.
     * <p>
     * See https://bugs.mysql.com/bug.php?id=53341
     * <p>
     * {@inheritDoc}
     */
    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return currentLevel;
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        requireNonNull(isolationLevel, "isolationLevel must not be null");

        // Set next transaction isolation level.
        return executeVoid(String.format("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql()))
            .doOnSuccess(ignored -> currentLevel = isolationLevel);
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

            return client.exchange(PingMessage.getInstance(), PING_DONE)
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

    @Override
    public boolean isAutoCommit() {
        return (context.getServerStatuses() & ServerStatuses.AUTO_COMMIT) != 0;
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return executeVoid(String.format("SET autocommit=%d", autoCommit ? 1 : 0));
    }

    boolean isInTransaction() {
        return (context.getServerStatuses() & ServerStatuses.IN_TRANSACTION) != 0;
    }

    private Mono<Void> executeVoid(String sql) {
        return QueryFlow.execute(client, sql).doOnNext(SAFE_RELEASE).then();
    }

    private Mono<Void> recoverIsolationLevel(Mono<Void> commitOrRollback) {
        if (currentLevel != sessionLevel) {
            // Need recover next transaction isolation level to session isolation level.
            return commitOrRollback.doOnSuccessOrError((ignored, throwable) -> {
                if (throwable == null || throwable instanceof R2dbcException) {
                    // Succeed or failed by server executing, just recover current isolation level.
                    currentLevel = sessionLevel;
                }
            });
        }

        return commitOrRollback;
    }

    /**
     * @param client  must be logged-in
     * @param context capabilities must be initialized
     * @param queryCache the cache of query
     * @param statementCache the cache of prepared statement identities.
     */
    static Mono<MySqlConnection> create(Client client, ConnectionContext context, @Nullable QueryCache queryCache, @Nullable StatementCache statementCache) {
        requireNonNull(client, "client must not be null");
        requireNonNull(context, "context must not be null");

        Codecs codecs = Codecs.getInstance();
        ServerVersion version = context.getServerVersion();
        String query;

        if (version.isGreaterThanOrEqualTo(TRAN_LEVEL_8x) || (version.isGreaterThanOrEqualTo(TRAN_LEVEL_5x) && version.isLessThan(TX_LEVEL_8x))) {
            query = "SELECT @@transaction_isolation AS i";
        } else {
            query = "SELECT @@tx_isolation AS i";
        }

        return new SimpleMySqlStatement(client, codecs, context, query)
            .execute()
            .flatMap(ISOLATION_LEVEL_HANDLER)
            .last()
            .map(level -> new MySqlConnection(client, context, codecs, level, queryCache, statementCache));
    }

    private static IsolationLevel convertIsolationLevel(@Nullable String name) {
        if (name == null) {
            logger.warn("Isolation level is null in current session, fallback to repeatable read");
            return IsolationLevel.REPEATABLE_READ;
        }

        switch (name) {
            case "READ-UNCOMMITTED":
                return IsolationLevel.READ_UNCOMMITTED;
            case "READ-COMMITTED":
                return IsolationLevel.READ_COMMITTED;
            case "REPEATABLE-READ":
                return IsolationLevel.REPEATABLE_READ;
            case "SERIALIZABLE":
                return IsolationLevel.SERIALIZABLE;
            default:
                logger.warn("Unknown isolation level {} in current session, fallback to repeatable read", name);
                return IsolationLevel.REPEATABLE_READ;
        }
    }
}
