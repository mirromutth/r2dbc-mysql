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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.message.client.PingMessage;
import dev.miku.r2dbc.mysql.message.server.CompleteMessage;
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import dev.miku.r2dbc.mysql.util.ServerVersion;
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
import java.util.function.Function;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireValidName;

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
    private static final ServerVersion TRAN_LEVEL_8X = ServerVersion.create(8, 0, 3);

    private static final ServerVersion TRAN_LEVEL_5X = ServerVersion.create(5, 7, 20);

    private static final ServerVersion TX_LEVEL_8X = ServerVersion.create(8, 0, 0);

    private static final Predicate<ServerMessage> PING_DONE = message ->
        message instanceof ErrorMessage || (message instanceof CompleteMessage && ((CompleteMessage) message).isDone());

    /**
     * Convert initialize result to {@link InitData}.
     */
    private static final Function<MySqlResult, Publisher<InitData>> INIT_HANDLER = r ->
        r.map((row, meta) -> new InitData(convertIsolationLevel(row.get(0, String.class)), row.get(1, String.class)));

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

    private final MySqlConnectionMetadata metadata;

    private final IsolationLevel sessionLevel;

    private final boolean deprecateEof;

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

    private MySqlConnection(Client client, ConnectionContext context, Codecs codecs, InitData data) {
        this.client = client;
        this.context = context;
        this.deprecateEof = (this.context.getCapabilities() & Capabilities.DEPRECATE_EOF) != 0;
        this.sessionLevel = data.level;
        this.currentLevel = data.level;
        this.codecs = codecs;
        this.metadata = new MySqlConnectionMetadata(context.getServerVersion().toString(), data.product);
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
                return QueryFlow.executeVoid(client, "START TRANSACTION");
            } else if (batchSupported) {
                return QueryFlow.executeVoid(client, "SET autocommit=0;START TRANSACTION");
            } else {
                return QueryFlow.executeVoid(client, "SET autocommit=0", "START TRANSACTION");
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
                commit = QueryFlow.executeVoid(client, "COMMIT");
            } else if (batchSupported) {
                commit = QueryFlow.executeVoid(client, "COMMIT;SET autocommit=1");
            } else {
                commit = QueryFlow.executeVoid(client, "COMMIT", "SET autocommit=1");
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
                return QueryFlow.executeVoid(client, sql);
            }

            // See TestKit.savePointStartsTransaction, if connection does not in transaction, then starts transaction.
            if (batchSupported) {
                if (isAutoCommit()) {
                    return QueryFlow.executeVoid(client, "SET autocommit=0;START TRANSACTION;" + sql);
                } else {
                    return QueryFlow.executeVoid(client, "START TRANSACTION;" + sql);
                }
            } else {
                if (isAutoCommit()) {
                    return QueryFlow.executeVoid(client, "SET autocommit=0", "START TRANSACTION", sql);
                } else {
                    return QueryFlow.executeVoid(client, "START TRANSACTION", sql);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     *
     * @param sql the SQL of the statement, should include only one-statement, otherwise stream terminate disordered.
     */
    @Override
    public MySqlStatement createStatement(String sql) {
        requireNonNull(sql, "sql must not be null");

        Query query = Query.parse(sql);

        if (query.isPrepared()) {
            logger.debug("Create a statement provided by prepare query");
            return new ParametrizedMySqlStatement(client, codecs, context, query, deprecateEof);
        } else {
            logger.debug("Create a statement provided by simple query");
            return new SimpleMySqlStatement(client, codecs, context, sql);
        }
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        requireValidName(name, "Savepoint name must not be empty and not contain backticks");

        return QueryFlow.executeVoid(client, String.format("RELEASE SAVEPOINT `%s`", name));
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return Mono.defer(() -> {
            if (!isInTransaction()) {
                return Mono.empty();
            }

            Mono<Void> rollback;

            if (isAutoCommit()) {
                rollback = QueryFlow.executeVoid(client, "ROLLBACK");
            } else if (batchSupported) {
                rollback = QueryFlow.executeVoid(client, "ROLLBACK;SET autocommit=1");
            } else {
                rollback = QueryFlow.executeVoid(client, "ROLLBACK", "SET autocommit=1");
            }

            return recoverIsolationLevel(rollback);
        });
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        requireValidName(name, "Savepoint name must not be empty and not contain backticks");

        return QueryFlow.executeVoid(client, String.format("ROLLBACK TO SAVEPOINT `%s`", name));
    }

    @Override
    public MySqlConnectionMetadata getMetadata() {
        return metadata;
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
        return QueryFlow.executeVoid(client, String.format("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql()))
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
        return QueryFlow.executeVoid(client, String.format("SET autocommit=%d", autoCommit ? 1 : 0));
    }

    boolean isInTransaction() {
        return (context.getServerStatuses() & ServerStatuses.IN_TRANSACTION) != 0;
    }

    private Mono<Void> recoverIsolationLevel(Mono<Void> commitOrRollback) {
        if (currentLevel != sessionLevel) {
            // Need recover next transaction isolation level to session isolation level.
            // Succeed or failed by server executing, just recover current isolation level.
            return commitOrRollback.doOnSuccess(ignored -> currentLevel = sessionLevel)
                .doOnError(e -> {
                    if (e instanceof R2dbcException) {
                        currentLevel = sessionLevel;
                    }
                });
        }

        return commitOrRollback;
    }

    /**
     * @param client  must be logged-in
     * @param context capabilities must be initialized
     */
    static Mono<MySqlConnection> create(Client client, ConnectionContext context) {
        requireNonNull(client, "client must not be null");
        requireNonNull(context, "context must not be null");

        Codecs codecs = Codecs.getInstance();
        ServerVersion version = context.getServerVersion();
        String query;

        // Maybe create a InitFlow for data initialization after login?
        if (version.isGreaterThanOrEqualTo(TRAN_LEVEL_8X) || (version.isGreaterThanOrEqualTo(TRAN_LEVEL_5X) && version.isLessThan(TX_LEVEL_8X))) {
            query = "SELECT @@transaction_isolation AS i, @@version_comment AS v";
        } else {
            query = "SELECT @@tx_isolation AS i, @@version_comment AS v";
        }

        return new SimpleMySqlStatement(client, codecs, context, query)
            .execute()
            .flatMap(INIT_HANDLER)
            .last()
            .map(data -> new MySqlConnection(client, context, codecs, data));
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

    private static class InitData {

        private final IsolationLevel level;

        @Nullable
        private final String product;

        private InitData(IsolationLevel level, @Nullable String product) {
            this.level = level;
            this.product = product;
        }
    }
}
