/*
 * Copyright 2018-2020 the original author or authors.
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
import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.message.client.AuthResponse;
import dev.miku.r2dbc.mysql.message.client.ClientMessage;
import dev.miku.r2dbc.mysql.message.client.HandshakeResponse;
import dev.miku.r2dbc.mysql.message.client.LoginClientMessage;
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
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A message flow considers both of parametrized and text queries, such as
 * {@link TextParametrizedStatement}, {@link PrepareParametrizedStatement},
 * {@link TextSimpleStatement}, {@link PrepareSimpleStatement} and
 * {@link MySqlBatch}.
 */
final class QueryFlow {

    // Metadata EOF message will be not receive in here.
    private static final Predicate<ServerMessage> RESULT_DONE = message -> message instanceof CompleteMessage;

    private static final Consumer<Object> OBJ_RELEASE = ReferenceCountUtil::release;

    /**
     * Login a {@link Client} and receive the {@code client} after logon.
     *
     * @param client   the {@link Client} to exchange messages with.
     * @param sslMode  the {@link SslMode} defines SSL capability and behavior.
     * @param database the database that will be connected.
     * @param user     the user that will be login.
     * @param password the password of the {@code user}.
     * @param context  the {@link ConnectionContext} for initialization.
     * @return the {@link Client}, or an error/exception received by login failed.
     */
    static Mono<Client> login(Client client, SslMode sslMode, String database, String user, @Nullable CharSequence password, ConnectionContext context) {
        return client.exchange(new LoginExchangeable(client, sslMode, database, user, password, context))
            .onErrorResume(e -> client.forceClose().then(Mono.error(e)))
            .then(Mono.just(client));
    }

    /**
     * Execute multiple bindings of a server-preparing statement with one-by-one binary execution.
     * The execution terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}.
     * The {@link ErrorMessage} will emit an exception and cancel subsequent {@link Binding}s.
     *
     * @param client    the {@link Client} to exchange messages with.
     * @param sql       the original statement for exception tracing.
     * @param bindings  the data of bindings.
     * @param fetchSize the size of fetching, if it less than or equal to {@literal 0} means fetch all rows.
     * @param cache     the cache of server-preparing result.
     * @return the messages received in response to this exchange, and will be completed
     * by {@link CompleteMessage} when it is last result for each binding.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql, List<Binding> bindings, int fetchSize, PrepareCache cache) {
        return Flux.defer(() -> {
            if (bindings.isEmpty()) {
                return Flux.empty();
            }

            return client.exchange(new PrepareExchangeable(cache, sql, bindings.iterator(), fetchSize))
                .windowUntil(RESULT_DONE);
        });
    }

    /**
     * Execute multiple bindings of a client-preparing statement with one-by-one text query.
     * The execution terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}.
     * The {@link ErrorMessage} will emit an exception and cancel subsequent {@link Binding}s.
     *
     * @param client   the {@link Client} to exchange messages with.
     * @param query    the {@link Query} for synthetic client-preparing statement.
     * @param bindings the data of bindings.
     * @return the messages received in response to this exchange, and will be completed
     * by {@link CompleteMessage} when it is last result for each binding.
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
     * Execute a simple query and return a {@link Mono} for the complete signal or error. Query execution terminates with
     * the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange, and will be
     * completed by {@link CompleteMessage} for each statement.
     */
    static Mono<Void> executeVoid(Client client, String sql) {
        return Mono.defer(() -> execute0(client, sql).doOnNext(OBJ_RELEASE).then());
    }

    /**
     * Execute multiple simple queries with one-by-one and return a {@link Mono} for the complete signal or
     * error. Query execution terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}.
     * The {@link ErrorMessage} will emit an exception and cancel subsequent statements execution.
     *
     * @param client     the {@link Client} to exchange messages with.
     * @param statements the queries to execute, each element can be contains multi-statements.
     * @return the messages received in response to this exchange, and will be
     * completed by {@link CompleteMessage} for each statement.
     */
    static Mono<Void> executeVoid(Client client, String... statements) {
        return client.exchange(new MultiQueryExchangeable(InternalArrays.asIterator(statements)))
            .doOnNext(OBJ_RELEASE)
            .then();
    }

    /**
     * Execute a simple compound query and return its results. Query execution terminates with
     * each {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage} will
     * emit an exception.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange, and will be completed by {@link CompleteMessage} when it is the last.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql) {
        return Flux.defer(() -> execute0(client, sql).windowUntil(RESULT_DONE));
    }

    /**
     * Execute multiple simple compound queries with one-by-one and return their results. Query execution
     * terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage}
     * will emit an exception and cancel subsequent statements execution.
     *
     * @param client     the {@link Client} to exchange messages with.
     * @param statements bundled sql for execute.
     * @return the messages received in response to this exchange, and will be
     * completed by {@link CompleteMessage} for each statement.
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
     * Execute a simple query. Query execution terminates with the last {@link CompleteMessage} or
     * a {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange, and will be completed by {@link CompleteMessage} when it is the last.
     */
    private static Flux<ServerMessage> execute0(Client client, String sql) {
        return client.exchange(TextQueryMessage.of(sql), (message, sink) -> {
            if (message instanceof ErrorMessage) {
                sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
            } else {
                sink.next(message);

                if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
                    sink.complete();
                }
            }
        });
    }

    private QueryFlow() {
    }
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
            sink.error(transform((ErrorMessage) message));
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

    abstract protected R2dbcException transform(ErrorMessage message);
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
        this.requests.onNext(binding.toTextMessage(this.query));
    }

    @Override
    protected boolean hasNext() {
        return bindings.hasNext();
    }

    @Override
    protected ClientMessage nextMessage() {
        return bindings.next().toTextMessage(query);
    }

    @Override
    protected R2dbcException transform(ErrorMessage message) {
        return ExceptionFactory.createException(message, query.getFormattedSql());
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
        this.requests.onNext(TextQueryMessage.of(current));
        this.current = current;
    }

    @Override
    protected boolean hasNext() {
        return statements.hasNext();
    }

    @Override
    protected ClientMessage nextMessage() {
        String current = statements.next();
        this.current = current;
        return TextQueryMessage.of(current);
    }

    @Override
    protected R2dbcException transform(ErrorMessage message) {
        return ExceptionFactory.createException(message, current);
    }
}

/**
 * An implementation of {@link FluxExchangeable} that considers server-preparing queries.
 * Which contains a built-in state machine.
 * <p>
 * It will reset a prepared statement if cache has matched it, otherwise it will prepare
 * statement to a new statement ID and put the ID into the cache. If the statement ID does
 * not exist in the cache after the last row sent, the ID will be closed.
 */
final class PrepareExchangeable extends FluxExchangeable<ServerMessage> {

    private static final Logger logger = LoggerFactory.getLogger(PrepareExchangeable.class);

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
            sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
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
                } else if (message instanceof SyntheticMetadataMessage && ((SyntheticMetadataMessage) message).isCompleted()) {
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
                    // Complete message means execute phase done or fetch phase done (when cursor is not opened).
                    onCompleteMessage((CompleteMessage) message, sink);
                } else if (message instanceof SyntheticMetadataMessage) {
                    EofMessage eof = ((SyntheticMetadataMessage) message).getEof();
                    if (eof instanceof ServerStatusMessage) {
                        // Otherwise means cursor does not be opened, wait for end of row EOF message.
                        if ((((ServerStatusMessage) eof).getServerStatuses() & ServerStatuses.CURSOR_EXISTS) != 0) {
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
        requests.onNext(this.fetch == null ? (this.fetch = new PreparedFetchMessage(statementId, fetchSize)) : this.fetch);

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
            if ((statuses & ServerStatuses.CURSOR_EXISTS) != 0 && (statuses & ServerStatuses.LAST_ROW_SENT) == 0) {
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
 * Not like other {@link FluxExchangeable}s, it is started by a server-side message,
 * which should be a {@link HandshakeRequest}.
 */
final class LoginExchangeable extends FluxExchangeable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(LoginExchangeable.class);

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

    LoginExchangeable(
        Client client, SslMode sslMode, String database, String user,
        @Nullable CharSequence password, ConnectionContext context
    ) {
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
    public void accept(ServerMessage message, SynchronousSink<Void> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
            return;
        }

        // Ensures it will be initialized only once.
        if (handshake) {
            handshake = false;
            if (message instanceof HandshakeRequest) {
                HandshakeRequest request = (HandshakeRequest) message;
                int capabilities = initHandshake(request);

                lastEnvelopeId = request.getEnvelopeId() + 1;

                if ((capabilities & Capabilities.SSL) == 0) {
                    requests.onNext(createHandshakeResponse(lastEnvelopeId, capabilities));
                } else {
                    requests.onNext(SslRequest.from(lastEnvelopeId, capabilities, context.getClientCollation().getId()));
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
            requests.onNext(createHandshakeResponse(++lastEnvelopeId, context.getCapabilities()));
        } else if (message instanceof AuthMoreDataMessage) {
            AuthMoreDataMessage msg = (AuthMoreDataMessage) message;
            lastEnvelopeId = msg.getEnvelopeId() + 1;

            if (msg.isFailed()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Connection (id {}) fast authentication failed, auto-try to use full authentication", context.getConnectionId());
                }

                requests.onNext(createAuthResponse(lastEnvelopeId, "full authentication"));
            }
            // Otherwise success, wait until OK message or Error message.
        } else if (message instanceof ChangeAuthMessage) {
            ChangeAuthMessage msg = (ChangeAuthMessage) message;
            lastEnvelopeId = msg.getEnvelopeId() + 1;
            authProvider = MySqlAuthProvider.build(msg.getAuthType());
            salt = msg.getSalt();
            requests.onNext(createAuthResponse(lastEnvelopeId,"change authentication"));
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
            throw new R2dbcPermissionDeniedException(formatAuthFails(authProvider.getType(), phase), CLI_SPECIFIC);
        }

        return new AuthResponse(envelopeId, authProvider.authentication(password, salt, context.getClientCollation()));
    }

    private int clientCapabilities(int serverCapabilities) {
        // Remove unknown flags.
        int capabilities = serverCapabilities & Capabilities.ALL_SUPPORTED;

        if (sslMode == SslMode.TUNNEL) {
            // Tunnel does not use MySQL SSL protocol, disable it.
            capabilities &= ~Capabilities.SSL;
        } else if ((capabilities & Capabilities.SSL) == 0) {
            // Server unsupported SSL.
            if (sslMode.requireSsl()) {
                throw new R2dbcPermissionDeniedException("Server version '" + context.getServerVersion() +
                    "' does not support SSL but mode '" + sslMode + "' requires SSL", CLI_SPECIFIC);
            } else if (sslMode.startSsl()) {
                // SSL has start yet, and client can be disable SSL, disable now.
                client.sslUnsupported();
            }
        } else {
            // Server supports SSL.
            if (!sslMode.startSsl()) {
                // SSL does not start, just remove flag.
                capabilities &= ~Capabilities.SSL;
            }

            if (!sslMode.verifyCertificate()) {
                // No need verify server cert, remove flag.
                capabilities &= ~Capabilities.SSL_VERIFY_SERVER_CERT;
            }
        }

        if (database.isEmpty() && (capabilities & Capabilities.CONNECT_WITH_DB) != 0) {
            capabilities &= ~Capabilities.CONNECT_WITH_DB;
        }

        if (ATTRIBUTES.isEmpty() && (capabilities & Capabilities.CONNECT_ATTRS) != 0) {
            capabilities &= ~Capabilities.CONNECT_ATTRS;
        }

        return capabilities;
    }

    private int initHandshake(HandshakeRequest message) {
        HandshakeHeader header = message.getHeader();
        int handshakeVersion = header.getProtocolVersion();
        ServerVersion serverVersion = header.getServerVersion();

        if (handshakeVersion < HANDSHAKE_VERSION) {
            logger.warn("The MySQL use old handshake V{}, server version is {}, maybe most features are not available", handshakeVersion, serverVersion);
        }

        int capabilities = clientCapabilities(message.getServerCapabilities());

        // No need initialize server statuses because it has initialized by read filter.
        this.context.init(header.getConnectionId(), serverVersion, capabilities);
        this.authProvider = MySqlAuthProvider.build(message.getAuthType());
        this.salt = message.getSalt();

        return capabilities;
    }

    private MySqlAuthProvider getAndNextProvider() {
        MySqlAuthProvider authProvider = this.authProvider;
        this.authProvider = authProvider.next();
        return authProvider;
    }

    private HandshakeResponse createHandshakeResponse(int envelopeId, int capabilities) {
        MySqlAuthProvider authProvider = getAndNextProvider();

        if (authProvider.isSslNecessary() && !sslCompleted) {
            throw new R2dbcPermissionDeniedException(formatAuthFails(authProvider.getType(), "handshake"), CLI_SPECIFIC);
        }

        byte[] authorization = authProvider.authentication(password, salt, context.getClientCollation());
        String authType = authProvider.getType();

        if (MySqlAuthProvider.NO_AUTH_PROVIDER.equals(authType)) {
            // Authentication type is not matter because of it has no authentication type.
            // Server need send a Change Authentication Message after handshake response.
            authType = MySqlAuthProvider.CACHING_SHA2_PASSWORD;
        }

        return HandshakeResponse.from(envelopeId, capabilities, context.getClientCollation().getId(),
            user, authorization, authType, database, ATTRIBUTES);
    }

    private static String formatAuthFails(String authType, String phase) {
        return "Authentication type '" + authType + "' must require SSL in " + phase + " phase";
    }
}
