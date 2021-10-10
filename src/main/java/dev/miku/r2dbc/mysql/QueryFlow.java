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

import dev.miku.r2dbc.mysql.authentication.MySqlAuthProvider;
import dev.miku.r2dbc.mysql.cache.PrepareCache;
import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.client.FluxExchangeable;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.message.client.AuthResponse;
import dev.miku.r2dbc.mysql.message.client.ClientMessage;
import dev.miku.r2dbc.mysql.message.client.HandshakeResponse;
import dev.miku.r2dbc.mysql.message.client.LoginClientMessage;
import dev.miku.r2dbc.mysql.message.client.PingMessage;
import dev.miku.r2dbc.mysql.message.client.PrepareQueryMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedCloseMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedFetchMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedResetMessage;
import dev.miku.r2dbc.mysql.message.client.SslRequest;
import dev.miku.r2dbc.mysql.message.client.TextQueryMessage;
import dev.miku.r2dbc.mysql.message.server.AuthMoreDataMessage;
import dev.miku.r2dbc.mysql.message.server.ChangeAuthMessage;
import dev.miku.r2dbc.mysql.message.server.CompleteMessage;
import dev.miku.r2dbc.mysql.message.server.EofMessage;
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.HandshakeHeader;
import dev.miku.r2dbc.mysql.message.server.HandshakeRequest;
import dev.miku.r2dbc.mysql.message.server.OkMessage;
import dev.miku.r2dbc.mysql.message.server.PreparedOkMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.ServerStatusMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticSslResponseMessage;
import dev.miku.r2dbc.mysql.util.InternalArrays;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.TransactionDefinition;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SynchronousSink;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A message flow considers both of parametrized and text queries, such as {@link TextParametrizedStatement},
 * {@link PrepareParametrizedStatement}, {@link TextSimpleStatement}, {@link PrepareSimpleStatement} and
 * {@link MySqlBatch}.
 */
final class QueryFlow {

    // Metadata EOF message will be not receive in here.
    private static final Predicate<ServerMessage> RESULT_DONE = message -> message instanceof CompleteMessage;

    private static final Consumer<ServerMessage> EXECUTE_VOID = message -> {
        if (message instanceof ErrorMessage) {
            throw ((ErrorMessage) message).toException();
        } else if (message instanceof ReferenceCounted) {
            ReferenceCountUtil.safeRelease(message);
        }
    };

    /**
     * Execute multiple bindings of a server-preparing statement with one-by-one binary execution. The
     * execution terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}. If client
     * receives a {@link ErrorMessage} will cancel subsequent {@link Binding}s. The exchange will be
     * completed by {@link CompleteMessage} after receive the last result for the last binding.
     *
     * @param client    the {@link Client} to exchange messages with.
     * @param sql       the original statement for exception tracing.
     * @param bindings  the data of bindings.
     * @param fetchSize the size of fetching, if it less than or equal to {@literal 0} means fetch all rows.
     * @param cache     the cache of server-preparing result.
     * @return the messages received in response to this exchange.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql, List<Binding> bindings, int fetchSize,
        PrepareCache cache) {
        return Flux.defer(() -> {
            if (bindings.isEmpty()) {
                return Flux.empty();
            }

            // Note: the prepared SQL may not be sent when the cache matches.
            return client.exchange(new PrepareExchangeable(cache, sql, bindings.iterator(), fetchSize))
                .windowUntil(RESULT_DONE);
        });
    }

    /**
     * Execute multiple bindings of a client-preparing statement with one-by-one text query. The execution
     * terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage}
     * will emit an exception and cancel subsequent {@link Binding}s. This exchange will be completed by
     * {@link CompleteMessage} after receive the last result for the last binding.
     *
     * @param client   the {@link Client} to exchange messages with.
     * @param query    the {@link Query} for synthetic client-preparing statement.
     * @param bindings the data of bindings.
     * @return the messages received in response to this exchange.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, Query query, List<Binding> bindings) {
        return Flux.defer(() -> {
            if (bindings.isEmpty()) {
                return Flux.empty();
            }

            return client.exchange(new TextQueryExchangeable(query, bindings.iterator()))
                .windowUntil(RESULT_DONE);
        });
    }

    /**
     * Execute a simple compound query. Query execution terminates with the last {@link CompleteMessage} or a
     * {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception. The exchange will be completed
     * by {@link CompleteMessage} after receive the last result for the last binding.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql) {
        return Flux.defer(() -> execute0(client, sql).windowUntil(RESULT_DONE));
    }

    /**
     * Execute multiple simple compound queries with one-by-one. Query execution terminates with the last
     * {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception and
     * cancel subsequent statements execution. The exchange will be completed by {@link CompleteMessage} after
     * receive the last result for the last binding.
     *
     * @param client     the {@link Client} to exchange messages with.
     * @param statements bundled sql for execute.
     * @return the messages received in response to this exchange.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, List<String> statements) {
        return Flux.defer(() -> {
            switch (statements.size()) {
                case 0:
                    return Flux.empty();
                case 1:
                    return execute0(client, statements.get(0)).windowUntil(RESULT_DONE);
                default:
                    return client.exchange(new MultiQueryExchangeable(statements.iterator()))
                        .windowUntil(RESULT_DONE);
            }
        });
    }

    /**
     * Login a {@link Client} and receive the {@code client} after logon. It will emit an exception when
     * client receives a {@link ErrorMessage}.
     *
     *
     * @param client   the {@link Client} to exchange messages with.
     * @param sslMode  the {@link SslMode} defines SSL capability and behavior.
     * @param database the database that will be connected.
     * @param user     the user that will be login.
     * @param password the password of the {@code user}.
     * @param context  the {@link ConnectionContext} for initialization.
     * @return the messages received in response to the login exchange.
     */
    static Mono<Client> login(Client client, SslMode sslMode, String database, String user,
        @Nullable CharSequence password, ConnectionContext context) {
        return client.exchange(new LoginExchangeable(client, sslMode, database, user, password, context))
            .flatMap(message -> client.forceClose().then(Mono.error(message::toException)))
            .then(Mono.just(client));
    }

    /**
     * Execute a simple query and return a {@link Mono} for the complete signal or error. Query execution
     * terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage}
     * will emit an exception. The exchange will be completed by {@link CompleteMessage} after receive the
     * last result for the last binding.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return receives complete signal.
     */
    static Mono<Void> executeVoid(Client client, String sql) {
        return Mono.defer(() -> execute0(client, sql).doOnNext(EXECUTE_VOID).then());
    }

    /**
     * Execute multiple simple queries with one-by-one and return a {@link Mono} for the complete signal or
     * error. Query execution terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}. The
     * {@link ErrorMessage} will emit an exception and cancel subsequent statements execution. The exchange
     * will be completed by {@link CompleteMessage} after receive the last result for the last binding.
     *
     * @param client     the {@link Client} to exchange messages with.
     * @param statements the queries to execute, each element can be contains multi-statements.
     * @return receives complete signal.
     */
    static Mono<Void> executeVoid(Client client, String... statements) {
        return client.exchange(new MultiQueryExchangeable(InternalArrays.asIterator(statements)))
            .doOnNext(EXECUTE_VOID)
            .then();
    }

    /**
     * Begins a new transaction with a {@link TransactionDefinition}.  It will change current transaction
     * statuses of the {@link ConnectionState}.
     *
     * @param client         the {@link Client} to exchange messages with.
     * @param state          the connection state for checks and sets transaction statuses.
     * @param batchSupported if connection supports batch query.
     * @param definition     the {@link TransactionDefinition}.
     * @return receives complete signal.
     */
    static Mono<Void> beginTransaction(Client client, ConnectionState state, boolean batchSupported,
        TransactionDefinition definition) {
        StartTransactionState startState = StartTransactionState.of(state, definition);

        if (batchSupported || startState.isSimple()) {
            return client.exchange(new TransactionBatchExchangeable(startState)).then();
        }

        return client.exchange(new TransactionMultiExchangeable(startState)).then();
    }

    /**
     * Commits or rollbacks current transaction.  It will recover statuses of the {@link ConnectionState} in
     * the initial connection state.
     *
     * @param client          the {@link Client} to exchange messages with.
     * @param state           the connection state for checks and resets transaction statuses.
     * @param commit          if commit, otherwise rollback.
     * @param lockWaitTimeout the lock wait timeout of the initial connection state.
     * @param batchSupported  if connection supports batch query.
     * @return receives complete signal.
     */
    static Mono<Void> doneTransaction(Client client, ConnectionState state, boolean commit,
        long lockWaitTimeout, boolean batchSupported) {
        CommitRollbackState commitState = CommitRollbackState.of(state, commit, lockWaitTimeout);

        if (batchSupported || commitState.isSimple()) {
            return client.exchange(new TransactionBatchExchangeable(commitState)).then();
        }

        return client.exchange(new TransactionMultiExchangeable(commitState)).then();
    }

    /**
     * Execute a simple query statement. Query execution terminates with the last {@link CompleteMessage} or a
     * {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception. The exchange will be completed
     * by {@link CompleteMessage} after receive the last result for the last binding. The exchange will be
     * completed by {@link CompleteMessage} after receive the last result for the last binding.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange.
     */
    private static Flux<ServerMessage> execute0(Client client, String sql) {
        return client.<ServerMessage>exchange(TextQueryMessage.of(sql), (message, sink) -> {
            if (message instanceof ErrorMessage) {
                sink.next(((ErrorMessage) message).offendedBy(sql));
                sink.complete();
            } else {
                sink.next(message);

                if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
                    sink.complete();
                }
            }
        }).doOnSubscribe(ignored -> QueryLogger.log(sql));
    }

    private QueryFlow() { }
}

/**
 * An abstraction of {@link FluxExchangeable} that considers multi-queries without binary protocols.
 */
abstract class BaseFluxExchangeable extends FluxExchangeable<ServerMessage> {

    protected final DirectProcessor<ClientMessage> requests = DirectProcessor.create();

    @Override
    public final void subscribe(CoreSubscriber<? super ClientMessage> actual) {
        requests.subscribe(actual);
        afterSubscribe();
    }

    @Override
    public final void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.next(((ErrorMessage) message).offendedBy(offendingSql()));
            sink.complete();
        } else {
            sink.next(message);

            if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
                if (!requests.isTerminated() && hasNext()) {
                    requests.onNext(nextMessage());
                } else {
                    sink.complete();
                }
            }
        }
    }

    abstract protected void afterSubscribe();

    abstract protected boolean hasNext();

    abstract protected ClientMessage nextMessage();

    abstract protected String offendingSql();
}

/**
 * An implementation of {@link FluxExchangeable} that considers client-preparing requests.
 */
final class TextQueryExchangeable extends BaseFluxExchangeable {

    private final AtomicBoolean disposed = new AtomicBoolean();

    private final Query query;

    private final Iterator<Binding> bindings;

    TextQueryExchangeable(Query query, Iterator<Binding> bindings) {
        this.query = query;
        this.bindings = bindings;
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            requests.onComplete();

            while (bindings.hasNext()) {
                bindings.next().clear();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }

    @Override
    protected void afterSubscribe() {
        Binding binding = this.bindings.next();

        QueryLogger.log(query);
        this.requests.onNext(binding.toTextMessage(this.query));
    }

    @Override
    protected boolean hasNext() {
        return bindings.hasNext();
    }

    @Override
    protected ClientMessage nextMessage() {
        QueryLogger.log(query);
        return bindings.next().toTextMessage(query);
    }

    @Override
    protected String offendingSql() {
        return query.getFormattedSql();
    }
}

/**
 * An implementation of {@link FluxExchangeable} that considers multiple simple statements.
 */
final class MultiQueryExchangeable extends BaseFluxExchangeable {

    private final Iterator<String> statements;

    private String current;

    MultiQueryExchangeable(Iterator<String> statements) {
        this.statements = statements;
    }

    @Override
    public void dispose() {
        requests.onComplete();
    }

    @Override
    public boolean isDisposed() {
        return requests.isTerminated();
    }

    @Override
    protected void afterSubscribe() {
        String current = this.statements.next();

        QueryLogger.log(current);
        this.current = current;
        this.requests.onNext(TextQueryMessage.of(current));
    }

    @Override
    protected boolean hasNext() {
        return statements.hasNext();
    }

    @Override
    protected ClientMessage nextMessage() {
        String current = statements.next();

        QueryLogger.log(current);
        this.current = current;
        return TextQueryMessage.of(current);
    }

    @Override
    protected String offendingSql() {
        return current;
    }
}

/**
 * An implementation of {@link FluxExchangeable} that considers server-preparing queries. Which contains a
 * built-in state machine.
 * <p>
 * It will reset a prepared statement if cache has matched it, otherwise it will prepare statement to a new
 * statement ID and put the ID into the cache. If the statement ID does not exist in the cache after the last
 * row sent, the ID will be closed.
 */
final class PrepareExchangeable extends FluxExchangeable<ServerMessage> {

    private static final Logger logger = Loggers.getLogger(PrepareExchangeable.class);

    private static final int PREPARE_OR_RESET = 1;

    private static final int EXECUTE = 2;

    private static final int FETCH = 3;

    private final AtomicBoolean disposed = new AtomicBoolean();

    private final DirectProcessor<ClientMessage> requests = DirectProcessor.create();

    private final PrepareCache cache;

    private final String sql;

    private final Iterator<Binding> bindings;

    private final int fetchSize;

    private int mode = PREPARE_OR_RESET;

    @Nullable
    private Integer statementId;

    @Nullable
    private PreparedFetchMessage fetch;

    private boolean shouldClose;

    PrepareExchangeable(PrepareCache cache, String sql, Iterator<Binding> bindings, int fetchSize) {
        this.cache = cache;
        this.sql = sql;
        this.bindings = bindings;
        this.fetchSize = fetchSize;
    }

    @Override
    public void subscribe(CoreSubscriber<? super ClientMessage> actual) {
        // It is also initialization method.
        requests.subscribe(actual);

        // After subscribe.
        Integer statementId = cache.getIfPresent(sql);
        if (statementId == null) {
            logger.debug("Prepare cache mismatch, try to preparing");
            this.shouldClose = true;
            QueryLogger.log(sql);
            this.requests.onNext(new PrepareQueryMessage(sql));
        } else {
            logger.debug("Prepare cache matched statement {} when getting", statementId);
            // Should reset only when it comes from cache.
            this.shouldClose = false;
            this.statementId = statementId;
            this.requests.onNext(new PreparedResetMessage(statementId));
        }
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.next(((ErrorMessage) message).offendedBy(sql));
            sink.complete();
            return;
        }

        switch (mode) {
            case PREPARE_OR_RESET:
                if (message instanceof OkMessage) {
                    // Reset succeed.
                    Integer statementId = this.statementId;
                    if (statementId == null) {
                        logger.error("Reset succeed but statement ID was null");
                        return;
                    }

                    doNextExecute(statementId, sink);
                } else if (message instanceof PreparedOkMessage) {
                    PreparedOkMessage ok = (PreparedOkMessage) message;
                    int statementId = ok.getStatementId();
                    int columns = ok.getTotalColumns();
                    int parameters = ok.getTotalParameters();

                    this.statementId = statementId;

                    // columns + parameters <= 0, has not metadata follow in,
                    if (columns <= -parameters) {
                        putToCache(statementId);
                        doNextExecute(statementId, sink);
                    }
                } else if (message instanceof SyntheticMetadataMessage &&
                    ((SyntheticMetadataMessage) message).isCompleted()) {
                    Integer statementId = this.statementId;
                    if (statementId == null) {
                        logger.error("Prepared OK message not found");
                        return;
                    }

                    putToCache(statementId);
                    doNextExecute(statementId, sink);
                } else {
                    ReferenceCountUtil.safeRelease(message);
                }
                // Ignore all messages in preparing phase.
                break;
            case EXECUTE:
                if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
                    // Complete message means execute or fetch phase done (when cursor is not opened).
                    onCompleteMessage((CompleteMessage) message, sink);
                } else if (message instanceof SyntheticMetadataMessage) {
                    EofMessage eof = ((SyntheticMetadataMessage) message).getEof();
                    if (eof instanceof ServerStatusMessage) {
                        // Otherwise means cursor does not be opened, wait for end of row EOF message.
                        if ((((ServerStatusMessage) eof).getServerStatuses() &
                            ServerStatuses.CURSOR_EXISTS) != 0) {
                            if (doNextFetch(sink)) {
                                sink.next(message);
                            }

                            break;
                        }
                    }
                    // EOF is deprecated (null) or using EOF without statuses.
                    // EOF is deprecated: wait for OK message.
                    // EOF without statuses: means cursor does not be opened, wait for end of row EOF message.
                    // Metadata message should be always emitted in EXECUTE phase.
                    setMode(FETCH);
                    sink.next(message);
                } else {
                    sink.next(message);
                }

                break;
            default:
                if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
                    onCompleteMessage((CompleteMessage) message, sink);
                } else {
                    sink.next(message);
                }
                break;
        }
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            if (!requests.isTerminated()) {
                Integer statementId = this.statementId;
                if (shouldClose && statementId != null) {
                    logger.debug("Closing statement {} after used", statementId);
                    requests.onNext(new PreparedCloseMessage(statementId));
                }
                requests.onComplete();
            }

            while (bindings.hasNext()) {
                bindings.next().clear();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }

    private void putToCache(Integer statementId) {
        boolean putSucceed;

        try {
            putSucceed = cache.putIfAbsent(sql, statementId, evictId -> {
                logger.debug("Prepare cache evicts statement {} when putting", evictId);
                requests.onNext(new PreparedCloseMessage(evictId));
            });
        } catch (Throwable e) {
            logger.error("Put statement {} to cache failed", statementId, e);
            putSucceed = false;
        }

        // If put failed, should close it.
        this.shouldClose = !putSucceed;
        logger.debug("Prepare cache put statement {} is {}", statementId, putSucceed ? "succeed" : "fails");
    }

    private void doNextExecute(int statementId, SynchronousSink<ServerMessage> sink) {
        if (requests.isTerminated()) {
            sink.complete();
            return;
        }

        Binding binding = bindings.next();

        setMode(EXECUTE);
        requests.onNext(binding.toExecuteMessage(statementId, fetchSize <= 0));
    }

    private boolean doNextFetch(SynchronousSink<ServerMessage> sink) {
        Integer statementId = this.statementId;

        if (statementId == null) {
            sink.error(new IllegalStateException("Statement ID must not be null when fetching"));
            return false;
        }

        if (requests.isTerminated()) {
            logger.error("Unexpected terminated on requests");
            sink.complete();
            return false;
        }

        setMode(FETCH);
        requests.onNext(this.fetch == null ? (this.fetch = new PreparedFetchMessage(statementId, fetchSize)) :
            this.fetch);

        return true;
    }

    private void setMode(int mode) {
        logger.debug("Mode is changed to {}", mode == EXECUTE ? "EXECUTE" : "FETCH");
        this.mode = mode;
    }

    private void onCompleteMessage(CompleteMessage message, SynchronousSink<ServerMessage> sink) {
        if (requests.isTerminated()) {
            logger.error("Unexpected terminated on requests");
            sink.next(message);
            sink.complete();
            return;
        }

        if (message instanceof ServerStatusMessage) {
            short statuses = ((ServerStatusMessage) message).getServerStatuses();
            if ((statuses & ServerStatuses.CURSOR_EXISTS) != 0 &&
                (statuses & ServerStatuses.LAST_ROW_SENT) == 0) {
                doNextFetch(sink);
                // Not last complete message, no need emit.
                return;
            }
            // Otherwise is last row sent or did not open cursor.
        }

        // The last row complete message should be emitted, whatever cursor has been opened.
        sink.next(message);

        if (bindings.hasNext()) {
            Integer statementId = this.statementId;

            if (statementId == null) {
                sink.error(new IllegalStateException("Statement ID must not be null when executing"));
                return;
            }

            doNextExecute(statementId, sink);
        } else {
            sink.complete();
        }
    }
}

/**
 * An implementation of {@link FluxExchangeable} that considers login to the database.
 * <p>
 * Not like other {@link FluxExchangeable}s, it is started by a server-side message, which should be an
 * implementation of {@link HandshakeRequest}.
 */
final class LoginExchangeable extends FluxExchangeable<ErrorMessage> {

    private static final Logger logger = Loggers.getLogger(LoginExchangeable.class);

    private static final Map<String, String> ATTRIBUTES = Collections.emptyMap();

    private static final String CLI_SPECIFIC = "HY000";

    private static final int HANDSHAKE_VERSION = 10;

    private final DirectProcessor<LoginClientMessage> requests = DirectProcessor.create();

    private final Client client;

    private final SslMode sslMode;

    private final String database;

    private final String user;

    @Nullable
    private final CharSequence password;

    private final ConnectionContext context;

    private boolean handshake = true;

    private MySqlAuthProvider authProvider;

    private byte[] salt;

    private boolean sslCompleted;

    private int lastEnvelopeId;

    LoginExchangeable(Client client, SslMode sslMode, String database, String user,
        @Nullable CharSequence password, ConnectionContext context) {
        this.client = client;
        this.sslMode = sslMode;
        this.database = database;
        this.user = user;
        this.password = password;
        this.context = context;
        this.sslCompleted = sslMode == SslMode.TUNNEL;
    }

    @Override
    public void subscribe(CoreSubscriber<? super ClientMessage> actual) {
        requests.subscribe(actual);
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<ErrorMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.next((ErrorMessage) message);
            sink.complete();
            return;
        }

        // Ensures it will be initialized only once.
        if (handshake) {
            handshake = false;
            if (message instanceof HandshakeRequest) {
                HandshakeRequest request = (HandshakeRequest) message;
                Capability capability = initHandshake(request);

                lastEnvelopeId = request.getEnvelopeId() + 1;

                if (capability.isSslEnabled()) {
                    requests.onNext(SslRequest.from(lastEnvelopeId, capability,
                        context.getClientCollation().getId()));
                } else {
                    requests.onNext(createHandshakeResponse(lastEnvelopeId, capability));
                }
            } else {
                sink.error(new R2dbcPermissionDeniedException("Unexpected message type '" +
                    message.getClass().getSimpleName() + "' in init phase"));
            }

            return;
        }

        if (message instanceof OkMessage) {
            client.loginSuccess();
            sink.complete();
        } else if (message instanceof SyntheticSslResponseMessage) {
            sslCompleted = true;
            requests.onNext(createHandshakeResponse(++lastEnvelopeId, context.getCapability()));
        } else if (message instanceof AuthMoreDataMessage) {
            AuthMoreDataMessage msg = (AuthMoreDataMessage) message;
            lastEnvelopeId = msg.getEnvelopeId() + 1;

            if (msg.isFailed()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Connection (id {}) fast authentication failed, use full authentication",
                        context.getConnectionId());
                }

                requests.onNext(createAuthResponse(lastEnvelopeId, "full authentication"));
            }
            // Otherwise success, wait until OK message or Error message.
        } else if (message instanceof ChangeAuthMessage) {
            ChangeAuthMessage msg = (ChangeAuthMessage) message;
            lastEnvelopeId = msg.getEnvelopeId() + 1;
            authProvider = MySqlAuthProvider.build(msg.getAuthType());
            salt = msg.getSalt();
            requests.onNext(createAuthResponse(lastEnvelopeId, "change authentication"));
        } else {
            sink.error(new R2dbcPermissionDeniedException("Unexpected message type '" +
                message.getClass().getSimpleName() + "' in login phase"));
        }
    }

    @Override
    public void dispose() {
        this.requests.onComplete();
    }

    private AuthResponse createAuthResponse(int envelopeId, String phase) {
        MySqlAuthProvider authProvider = getAndNextProvider();

        if (authProvider.isSslNecessary() && !sslCompleted) {
            throw new R2dbcPermissionDeniedException(formatAuthFails(authProvider.getType(), phase),
                CLI_SPECIFIC);
        }

        return new AuthResponse(envelopeId,
            authProvider.authentication(password, salt, context.getClientCollation()));
    }

    private Capability clientCapability(Capability serverCapability) {
        Capability.Builder builder = serverCapability.mutate();

        builder.disableDatabasePinned();
        builder.disableCompression();
        builder.disableLoadDataInfile();
        builder.disableIgnoreAmbiguitySpace();
        builder.disableInteractiveTimeout();

        if (sslMode == SslMode.TUNNEL) {
            // Tunnel does not use MySQL SSL protocol, disable it.
            builder.disableSsl();
        } else if (!serverCapability.isSslEnabled()) {
            // Server unsupported SSL.
            if (sslMode.requireSsl()) {
                throw new R2dbcPermissionDeniedException("Server version '" + context.getServerVersion() +
                    "' does not support SSL but mode '" + sslMode + "' requires SSL", CLI_SPECIFIC);
            } else if (sslMode.startSsl()) {
                // SSL has start yet, and client can be disable SSL, disable now.
                client.sslUnsupported();
            }
        } else {
            // The server supports SSL, but the user does not want to use SSL, disable it.
            if (!sslMode.startSsl()) {
                builder.disableSsl();
            }
        }

        if (database.isEmpty()) {
            builder.disableConnectWithDatabase();
        }

        if (ATTRIBUTES.isEmpty()) {
            builder.disableConnectAttributes();
        }

        return builder.build();
    }

    private Capability initHandshake(HandshakeRequest message) {
        HandshakeHeader header = message.getHeader();
        int handshakeVersion = header.getProtocolVersion();
        ServerVersion serverVersion = header.getServerVersion();

        if (handshakeVersion < HANDSHAKE_VERSION) {
            logger.warn("MySQL use handshake V{}, server version is {}, maybe most features are unavailable",
                handshakeVersion, serverVersion);
        }

        Capability capability = clientCapability(message.getServerCapability());

        // No need initialize server statuses because it has initialized by read filter.
        this.context.init(header.getConnectionId(), serverVersion, capability);
        this.authProvider = MySqlAuthProvider.build(message.getAuthType());
        this.salt = message.getSalt();

        return capability;
    }

    private MySqlAuthProvider getAndNextProvider() {
        MySqlAuthProvider authProvider = this.authProvider;
        this.authProvider = authProvider.next();
        return authProvider;
    }

    private HandshakeResponse createHandshakeResponse(int envelopeId, Capability capability) {
        MySqlAuthProvider authProvider = getAndNextProvider();

        if (authProvider.isSslNecessary() && !sslCompleted) {
            throw new R2dbcPermissionDeniedException(formatAuthFails(authProvider.getType(), "handshake"),
                CLI_SPECIFIC);
        }

        byte[] authorization = authProvider.authentication(password, salt, context.getClientCollation());
        String authType = authProvider.getType();

        if (MySqlAuthProvider.NO_AUTH_PROVIDER.equals(authType)) {
            // Authentication type is not matter because of it has no authentication type.
            // Server need send a Change Authentication Message after handshake response.
            authType = MySqlAuthProvider.CACHING_SHA2_PASSWORD;
        }

        return HandshakeResponse.from(envelopeId, capability, context.getClientCollation().getId(),
            user, authorization, authType, database, ATTRIBUTES);
    }

    private static String formatAuthFails(String authType, String phase) {
        return "Authentication type '" + authType + "' must require SSL in " + phase + " phase";
    }
}

abstract class AbstractTransactionState {

    final ConnectionState state;

    /**
     * A bitmap of unfinished tasks, the lowest one bit is current task.
     */
    int tasks;

    private final List<String> statements;

    @Nullable
    private String sql;

    protected AbstractTransactionState(ConnectionState state, int tasks, List<String> statements) {
        this.state = state;
        this.tasks = tasks;
        this.statements = statements;
    }

    final void setSql(String sql) {
        this.sql = sql;
    }

    final boolean isSimple() {
        return statements.size() == 1;
    }

    final String batchStatement() {
        if (statements.size() == 1) {
            return statements.get(0);
        }

        return String.join(";", statements);
    }

    final Iterator<String> statements() {
        return statements.iterator();
    }

    final boolean accept(ServerMessage message, SynchronousSink<Void> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(((ErrorMessage) message).toException(sql));
            return false;
        } else if (message instanceof CompleteMessage) {
            // Note: if CompleteMessage.isDone() is true, it is the last complete message of the entire
            // operation in batch mode and the last complete message of the current query in multi-query mode.
            // That means each complete message should be processed whatever it is done or not IN BATCH MODE.
            // And process only the complete message that's done IN MULTI-QUERY MODE.
            // So if we need to check isDone() here, should give an extra boolean variable: is it batch mode?
            // Currently, the tasks can determine current state, no need check isDone(). The Occam’s razor.
            int task = Integer.lowestOneBit(tasks);

            // Remove current task for prepare next task.
            this.tasks -= task;

            return process(task, sink);
        } else if (message instanceof ReferenceCounted) {
            ReferenceCountUtil.safeRelease(message);
        }

        return false;
    }

    abstract boolean cancelTasks();

    protected abstract boolean process(int task, SynchronousSink<Void> sink);
}

final class CommitRollbackState extends AbstractTransactionState {

    private static final int LOCK_WAIT_TIMEOUT = 1;

    private static final int COMMIT_OR_ROLLBACK = 2;

    private CommitRollbackState(ConnectionState state, int tasks, List<String> statements) {
        super(state, tasks, statements);
    }

    @Override
    boolean cancelTasks() {
        if (state.isInTransaction()) {
            return false;
        }

        this.tasks = COMMIT_OR_ROLLBACK;

        return true;
    }

    @Override
    protected boolean process(int task, SynchronousSink<Void> sink) {
        switch (task) {
            case LOCK_WAIT_TIMEOUT:
                state.resetCurrentLockWaitTimeout();
                return true;
            case COMMIT_OR_ROLLBACK:
                state.resetIsolationLevel();
                sink.complete();
                return false;
        }

        sink.error(new IllegalStateException("Undefined commit task: " + task + ", remain: " + tasks));

        return false;
    }

    static CommitRollbackState of(ConnectionState state, boolean commit, long lockWaitTimeout) {
        String doneSql = commit ? "COMMIT" : "ROLLBACK";

        if (state.isLockWaitTimeoutChanged()) {
            List<String> statements = new ArrayList<>(2);

            statements.add("SET innodb_lock_wait_timeout=" + lockWaitTimeout);
            statements.add(doneSql);

            return new CommitRollbackState(state, LOCK_WAIT_TIMEOUT | COMMIT_OR_ROLLBACK, statements);
        }

        return new CommitRollbackState(state, COMMIT_OR_ROLLBACK, Collections.singletonList(doneSql));
    }
}

final class StartTransactionState extends AbstractTransactionState {

    private static final int LOCK_WAIT_TIMEOUT = 1;

    private static final int ISOLATION_LEVEL = 2;

    private static final int START_TRANSACTION = 4;

    private final long lockWaitTimeout;

    @Nullable
    private final IsolationLevel isolationLevel;

    private StartTransactionState(ConnectionState state, int tasks, List<String> statements,
        long lockWaitTimeout, @Nullable IsolationLevel level) {
        super(state, tasks, statements);

        this.lockWaitTimeout = lockWaitTimeout;
        this.isolationLevel = level;
    }

    @Override
    boolean cancelTasks() {
        if (state.isInTransaction()) {
            this.tasks = START_TRANSACTION;
            return true;
        }

        return false;
    }

    @Override
    protected boolean process(int task, SynchronousSink<Void> sink) {
        switch (task) {
            case LOCK_WAIT_TIMEOUT:
                state.setCurrentLockWaitTimeout(lockWaitTimeout);
                return true;
            case ISOLATION_LEVEL:
                if (isolationLevel != null) {
                    state.setIsolationLevel(isolationLevel);
                }
                return true;
            case START_TRANSACTION:
                sink.complete();
                return false;
        }

        sink.error(new IllegalStateException("Undefined transaction task: " + task + ", remain: " + tasks));

        return false;
    }

    static StartTransactionState of(ConnectionState state, TransactionDefinition definition) {
        int tasks = START_TRANSACTION;
        Duration timeout = definition.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT);
        List<String> statements = null;
        long lockWaitTimeout;

        if (timeout == null) {
            lockWaitTimeout = Long.MIN_VALUE;
        } else {
            lockWaitTimeout = timeout.getSeconds();
            statements = new ArrayList<>(3);
            statements.add("SET innodb_lock_wait_timeout=" + lockWaitTimeout);
            tasks |= LOCK_WAIT_TIMEOUT;
        }

        IsolationLevel isolationLevel = definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL);

        if (isolationLevel != null) {
            if (statements == null) {
                statements = new ArrayList<>(3);
            }
            statements.add("SET TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql());
            tasks |= ISOLATION_LEVEL;
        }

        if (statements == null) {
            return new StartTransactionState(state, tasks,
                Collections.singletonList(buildStartTransaction(definition)), lockWaitTimeout, null);
        }

        statements.add(buildStartTransaction(definition));

        return new StartTransactionState(state, tasks, statements, lockWaitTimeout, isolationLevel);
    }

    private static String buildStartTransaction(TransactionDefinition definition) {
        Boolean readOnly = definition.getAttribute(TransactionDefinition.READ_ONLY);
        Boolean snapshot = definition.getAttribute(MySqlTransactionDefinition.WITH_CONSISTENT_SNAPSHOT);

        if (readOnly == null && (snapshot == null || !snapshot)) {
            return "BEGIN";
        }

        StringBuilder builder = new StringBuilder(90).append("START TRANSACTION");

        if (snapshot != null && snapshot) {
            ConsistentSnapshotEngine engine =
                definition.getAttribute(MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_ENGINE);

            builder.append(" WITH CONSISTENT ");

            if (engine == null) {
                builder.append("SNAPSHOT");
            } else {
                builder.append(engine.asSql()).append(" SNAPSHOT");
            }

            Long sessionId =
                definition.getAttribute(MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_FROM_SESSION);

            if (sessionId != null) {
                builder.append(" FROM SESSION ").append(Long.toUnsignedString(sessionId));
            }
        }

        if (readOnly != null) {
            if (readOnly) {
                builder.append(" READ ONLY");
            } else {
                builder.append(" READ WRITE");
            }
        }

        return builder.toString();
    }
}

final class TransactionBatchExchangeable extends FluxExchangeable<Void> {

    private final AbstractTransactionState state;

    TransactionBatchExchangeable(AbstractTransactionState state) {
        this.state = state;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<Void> sink) {
        state.accept(message, sink);
    }

    @Override
    public void dispose() {
        // Do nothing.
    }

    @Override
    public void subscribe(CoreSubscriber<? super ClientMessage> s) {
        if (state.cancelTasks()) {
            s.onSubscribe(Operators.scalarSubscription(s, PingMessage.INSTANCE));

            return;
        }

        String sql = state.batchStatement();

        QueryLogger.log(sql);
        state.setSql(sql);
        s.onSubscribe(Operators.scalarSubscription(s, TextQueryMessage.of(sql)));
    }
}

final class TransactionMultiExchangeable extends FluxExchangeable<Void> {

    private final DirectProcessor<ClientMessage> requests = DirectProcessor.create();

    private final AbstractTransactionState state;

    private final Iterator<String> statements;

    TransactionMultiExchangeable(AbstractTransactionState state) {
        this.state = state;
        this.statements = state.statements();
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<Void> sink) {
        if (state.accept(message, sink)) {
            String sql = statements.next();

            QueryLogger.log(sql);
            state.setSql(sql);
            requests.onNext(TextQueryMessage.of(sql));
        }
    }

    @Override
    public void dispose() {
        requests.onComplete();
    }

    @Override
    public void subscribe(CoreSubscriber<? super ClientMessage> s) {
        if (state.cancelTasks()) {
            s.onSubscribe(Operators.scalarSubscription(s, PingMessage.INSTANCE));

            return;
        }

        String sql = statements.next();

        QueryLogger.log(sql);
        state.setSql(sql);
        requests.subscribe(s);
        requests.onNext(TextQueryMessage.of(sql));
    }
}
