/*
 * Copyright 2018-2021 the original author or authors.
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

import dev.miku.r2dbc.mysql.cache.PrepareCache;
import dev.miku.r2dbc.mysql.cache.QueryCache;
import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.message.client.PingMessage;
import dev.miku.r2dbc.mysql.message.server.CompleteMessage;
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireValidName;

/**
 * An implementation of {@link Connection} for connecting to the MySQL database.
 */
public final class MySqlConnection implements Connection, ConnectionState {

    private static final Logger logger = Loggers.getLogger(MySqlConnection.class);

    private static final int DEFAULT_LOCK_WAIT_TIMEOUT = 50;

    private static final String ZONE_PREFIX_POSIX = "posix/";

    private static final String ZONE_PREFIX_RIGHT = "right/";

    private static final int PREFIX_LENGTH = 6;

    /**
     * If MySQL server version greater than or equal to {@literal 8.0.3}, or greater than or equal to
     * {@literal 5.7.20} and less than {@literal 8.0.0}, the column name of current session isolation level
     * will be {@literal @@transaction_isolation}, otherwise it is {@literal @@tx_isolation}.
     *
     * @see #init judge server version before get the isolation level.
     */
    private static final ServerVersion TRAN_LEVEL_8X = ServerVersion.create(8, 0, 3);

    private static final ServerVersion TRAN_LEVEL_5X = ServerVersion.create(5, 7, 20);

    private static final ServerVersion TX_LEVEL_8X = ServerVersion.create(8, 0, 0);

    /**
     * Convert initialize result to {@link InitData}.
     */
    private static final Function<MySqlResult, Publisher<InitData>> INIT_HANDLER = r ->
        r.map((row, meta) -> new InitData(convertIsolationLevel(row.get(0, String.class)),
            convertLockWaitTimeout(row.get(1, Long.class)),
            row.get(2, String.class), null));

    private static final Function<MySqlResult, Publisher<InitData>> FULL_INIT = r -> r.map((row, meta) -> {
        IsolationLevel level = convertIsolationLevel(row.get(0, String.class));
        long lockWaitTimeout = convertLockWaitTimeout(row.get(1, Long.class));
        String product = row.get(2, String.class);
        String systemTimeZone = row.get(3, String.class);
        String timeZone = row.get(4, String.class);
        ZoneId zoneId;

        if (timeZone == null || timeZone.isEmpty() || "SYSTEM".equalsIgnoreCase(timeZone)) {
            if (systemTimeZone == null || systemTimeZone.isEmpty()) {
                logger.warn("MySQL does not return any timezone, trying to use system default timezone");
                zoneId = ZoneId.systemDefault();
            } else {
                zoneId = convertZoneId(systemTimeZone);
            }
        } else {
            zoneId = convertZoneId(timeZone);
        }

        return new InitData(level, lockWaitTimeout, product, zoneId);
    });

    private static final BiConsumer<ServerMessage, SynchronousSink<Boolean>> PING = (message, sink) -> {
        if (message instanceof ErrorMessage) {
            ErrorMessage msg = (ErrorMessage) message;
            logger.debug("Remote validate failed: [{}] [{}] {}", msg.getCode(), msg.getSqlState(),
                msg.getMessage());
            sink.next(false);
            sink.complete();
        } else if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
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

    private final MySqlConnectionMetadata metadata;

    private final IsolationLevel sessionLevel;

    private final QueryCache queryCache;

    private final PrepareCache prepareCache;

    @Nullable
    private final Predicate<String> prepare;

    /**
     * Current isolation level inferred by past statements.
     * <p>
     * Inference rules:
     * <ol><li>In the beginning, it is also {@link #sessionLevel}.</li>
     * <li>After the user calls {@link #setTransactionIsolationLevel(IsolationLevel)}, it will change to
     * the user-specified value.</li>
     * <li>After the end of a transaction (commit or rollback), it will recover to {@link #sessionLevel}.</li>
     * </ol>
     */
    private volatile IsolationLevel currentLevel;

    /**
     * Session lock wait timeout.
     */
    private volatile long lockWaitTimeout;

    /**
     * Current transaction lock wait timeout.
     */
    private volatile long currentLockWaitTimeout;

    MySqlConnection(Client client, ConnectionContext context, Codecs codecs, IsolationLevel level,
        long lockWaitTimeout, QueryCache queryCache, PrepareCache prepareCache, @Nullable String product,
        @Nullable Predicate<String> prepare) {
        this.client = client;
        this.context = context;
        this.sessionLevel = level;
        this.currentLevel = level;
        this.codecs = codecs;
        this.lockWaitTimeout = lockWaitTimeout;
        this.currentLockWaitTimeout = lockWaitTimeout;
        this.queryCache = queryCache;
        this.prepareCache = prepareCache;
        this.metadata = new MySqlConnectionMetadata(context.getServerVersion().toString(), product);
        this.batchSupported = context.getCapability().isMultiStatementsAllowed();
        this.prepare = prepare;

        if (this.batchSupported) {
            logger.debug("Batch is supported by server");
        } else {
            logger.warn("The MySQL server does not support batch, fallback to executing one-by-one");
        }
    }

    @Override
    public Mono<Void> beginTransaction() {
        return Mono.defer(() -> {
            if (isInTransaction()) {
                return Mono.empty();
            }

            return QueryFlow.executeVoid(client, "BEGIN");
        });
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        return Mono.defer(() -> {
            if (isInTransaction()) {
                return Mono.empty();
            }

            return QueryFlow.beginTransaction(client, this, batchSupported, definition);
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

            return QueryFlow.doneTransaction(client, this, true, lockWaitTimeout, batchSupported);
        });
    }

    @Override
    public MySqlBatch createBatch() {
        return batchSupported ? new MySqlBatchingBatch(client, codecs, context) :
            new MySqlSyntheticBatch(client, codecs, context);

    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        requireValidName(name, "Savepoint name must not be empty and not contain backticks");

        String sql = String.format("SAVEPOINT `%s`", name);

        return Mono.defer(() -> {
            if (isInTransaction()) {
                return QueryFlow.executeVoid(client, sql);
            } else if (batchSupported) {
                // If connection does not in transaction, then starts transaction.
                return QueryFlow.executeVoid(client, "BEGIN;" + sql);
            }

            return QueryFlow.executeVoid(client, "BEGIN", sql);
        });
    }

    @Override
    public MySqlStatement createStatement(String sql) {
        requireNonNull(sql, "sql must not be null");

        Query query = queryCache.get(sql);

        if (query.isSimple()) {
            if (prepare != null && prepare.test(sql)) {
                logger.debug("Create a simple statement provided by prepare query");
                return new PrepareSimpleStatement(client, codecs, context, sql, prepareCache);
            }

            logger.debug("Create a simple statement provided by text query");

            return new TextSimpleStatement(client, codecs, context, sql);
        }

        if (prepare == null) {
            logger.debug("Create a parametrized statement provided by text query");
            return new TextParametrizedStatement(client, codecs, query, context);
        }

        logger.debug("Create a parametrized statement provided by prepare query");

        return new PrepareParametrizedStatement(client, codecs, query, context, prepareCache);
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

            return QueryFlow.doneTransaction(client, this, false, lockWaitTimeout, batchSupported);
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
     * MySQL does not have any way to query the isolation level of the current transaction, only inferred from
     * past statements, so driver can not make sure the result is right.
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
        return QueryFlow.executeVoid(client, "SET TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql())
            .doOnSuccess(ignored -> setIsolationLevel(isolationLevel));
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

            return client.exchange(PingMessage.INSTANCE, PING)
                .last()
                .onErrorResume(e -> {
                    // `last` maybe emit a NoSuchElementException, exchange maybe emit exception by Netty.
                    // But should NEVER emit any exception, so logging exception and emit false.
                    logger.debug("Remote validate failed", e);
                    return Mono.just(false);
                });
        });
    }

    @Override
    public boolean isAutoCommit() {
        // Within transaction, autocommit remains disabled until end the transaction with COMMIT or ROLLBACK.
        // The autocommit mode then reverts to its previous state.
        return !isInTransaction() && isSessionAutoCommit();
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return Mono.defer(() -> {
            if (autoCommit == isSessionAutoCommit()) {
                return Mono.empty();
            }

            return QueryFlow.executeVoid(client, "SET autocommit=" + (autoCommit ? 1 : 0));
        });
    }

    @Override
    public void setIsolationLevel(IsolationLevel level) {
        this.currentLevel = level;
    }

    @Override
    public void setCurrentLockWaitTimeout(long timeoutSeconds) {
        this.currentLockWaitTimeout = timeoutSeconds;
    }

    @Override
    public void resetIsolationLevel() {
        this.currentLevel = this.sessionLevel;
    }

    @Override
    public boolean isLockWaitTimeoutChanged() {
        return currentLockWaitTimeout != lockWaitTimeout;
    }

    @Override
    public void resetCurrentLockWaitTimeout() {
        this.currentLockWaitTimeout = this.lockWaitTimeout;
    }

    @Override
    public boolean isInTransaction() {
        return (context.getServerStatuses() & ServerStatuses.IN_TRANSACTION) != 0;
    }

    @Override
    public Mono<Void> setLockWaitTimeout(Duration timeout) {
        requireNonNull(timeout, "timeout must not be null");

        long timeoutSeconds = timeout.getSeconds();
        return QueryFlow.executeVoid(client, "SET innodb_lock_wait_timeout=" + timeoutSeconds)
            .doOnSuccess(ignored -> this.lockWaitTimeout = this.currentLockWaitTimeout = timeoutSeconds);
    }

    @Override
    public Publisher<Void> setStatementTimeout(Duration timeout) {
        requireNonNull(timeout, "timeout must not be null");

        // TODO: implement me
        return Mono.empty();
    }

    boolean isSessionAutoCommit() {
        return (context.getServerStatuses() & ServerStatuses.AUTO_COMMIT) != 0;
    }

    /**
     * Initialize a {@link MySqlConnection} after login.
     *
     * @param client       must be logged-in.
     * @param codecs       the {@link Codecs}.
     * @param context      must be initialized.
     * @param queryCache   the cache of {@link Query}.
     * @param prepareCache the cache of server-preparing result.
     * @param prepare      judging for prefer use prepare statement to execute simple query.
     * @return a {@link Mono} will emit an initialized {@link MySqlConnection}.
     */
    static Mono<MySqlConnection> init(Client client, Codecs codecs, ConnectionContext context,
        QueryCache queryCache, PrepareCache prepareCache, @Nullable Predicate<String> prepare) {
        ServerVersion version = context.getServerVersion();
        StringBuilder query = new StringBuilder(128);

        // Maybe create a InitFlow for data initialization after login?
        if (version.isGreaterThanOrEqualTo(TRAN_LEVEL_8X) ||
            (version.isGreaterThanOrEqualTo(TRAN_LEVEL_5X) && version.isLessThan(TX_LEVEL_8X))) {
            query.append(
                "SELECT @@transaction_isolation AS i,@@innodb_lock_wait_timeout AS l,@@version_comment AS v");
        } else {
            query.append("SELECT @@tx_isolation AS i,@@innodb_lock_wait_timeout AS l,@@version_comment AS v");
        }

        Function<MySqlResult, Publisher<InitData>> handler;

        if (context.shouldSetServerZoneId()) {
            query.append(",@@system_time_zone AS s,@@time_zone AS t");
            handler = FULL_INIT;
        } else {
            handler = INIT_HANDLER;
        }

        return new TextSimpleStatement(client, codecs, context, query.toString())
            .execute()
            .flatMap(handler)
            .last()
            .map(data -> {
                ZoneId serverZoneId = data.serverZoneId;
                if (serverZoneId != null) {
                    logger.debug("Set server time zone to {} from init query", serverZoneId);
                    context.setServerZoneId(serverZoneId);
                }

                return new MySqlConnection(client, context, codecs, data.level, data.lockWaitTimeout,
                    queryCache, prepareCache, data.product, prepare);
            });
    }

    /**
     * Creates a {@link ZoneId} from MySQL timezone result, or fallback to system default timezone if not
     * found.
     *
     * @param id the ID/name of MySQL timezone.
     * @return the {@link ZoneId}.
     */
    private static ZoneId convertZoneId(String id) {
        String realId;

        if (id.startsWith(ZONE_PREFIX_POSIX) || id.startsWith(ZONE_PREFIX_RIGHT)) {
            realId = id.substring(PREFIX_LENGTH);
        } else {
            realId = id;
        }

        try {
            switch (realId) {
                case "Factory":
                    // Looks like the "Factory" time zone is UTC.
                    return ZoneOffset.UTC;
                case "America/Nuuk":
                    // They are same timezone including DST.
                    return ZoneId.of("America/Godthab");
                case "ROC":
                    // Republic of China, 1912-1949, very very old time zone.
                    // Even the ZoneId.SHORT_IDS does not support it.
                    // Is there anyone using this time zone, really?
                    // Don't think so, but should support it for compatible.
                    // Just use GMT+8, id is equal to +08:00.
                    return ZoneId.of("+8");
            }

            return ZoneId.of(realId, ZoneId.SHORT_IDS);
        } catch (DateTimeException e) {
            logger.warn("The server timezone is unknown <{}>, trying to use system default timezone", id, e);

            return ZoneId.systemDefault();
        }
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
        }

        logger.warn("Unknown isolation level {} in current session, fallback to repeatable read", name);

        return IsolationLevel.REPEATABLE_READ;
    }

    private static long convertLockWaitTimeout(@Nullable Long timeout) {
        if (timeout == null) {
            logger.error("Lock wait timeout is null, fallback to " + DEFAULT_LOCK_WAIT_TIMEOUT + " seconds");

            return DEFAULT_LOCK_WAIT_TIMEOUT;
        }

        return timeout;
    }

    private static class InitData {

        private final IsolationLevel level;

        private final long lockWaitTimeout;

        @Nullable
        private final String product;

        @Nullable
        private final ZoneId serverZoneId;

        private InitData(IsolationLevel level, long lockWaitTimeout, @Nullable String product,
            @Nullable ZoneId serverZoneId) {
            this.level = level;
            this.lockWaitTimeout = lockWaitTimeout;
            this.product = product;
            this.serverZoneId = serverZoneId;
        }
    }
}
