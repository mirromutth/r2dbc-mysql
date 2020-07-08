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
import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.message.client.AuthResponse;
import dev.miku.r2dbc.mysql.message.client.ClientMessage;
import dev.miku.r2dbc.mysql.message.client.HandshakeResponse;
import dev.miku.r2dbc.mysql.message.client.PrepareQueryMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedCloseMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedFetchMessage;
import dev.miku.r2dbc.mysql.message.client.SimpleQueryMessage;
import dev.miku.r2dbc.mysql.message.client.SslRequest;
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
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
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

    private static final Predicate<ServerMessage> FETCH_DONE = message -> message instanceof ErrorMessage ||
        (message instanceof CompleteMessage && ((CompleteMessage) message).isDone());

    private static final Consumer<ReferenceCounted> RELEASE = ReferenceCounted::release;

    private static final Consumer<Object> OBJ_RELEASE = ReferenceCountUtil::release;

    /**
     * Login a {@link Client} and receive the {@link Client} after logon.
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
        EmitterProcessor<ClientMessage> requests = EmitterProcessor.create(1, false);
        InitHandler handler = new InitHandler(requests, client, sslMode, database, user, password, context);

        return client.exchange(requests, handler)
            .handle(handler)
            .onErrorResume(e -> {
                requests.onComplete();
                return client.forceClose().then(Mono.error(e));
            })
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
     * @return the messages received in response to this exchange, and will be completed
     * by {@link CompleteMessage} when it is last result for each binding.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql, List<Binding> bindings, int fetchSize) {
        return Flux.defer(() -> {
            if (bindings.isEmpty()) {
                return Flux.empty();
            }

            EmitterProcessor<ClientMessage> requests = EmitterProcessor.create(1, false);
            PrepareHandler handler = new PrepareHandler(requests, sql, bindings.iterator(), fetchSize);

            return OperatorUtils.discardOnCancel(client.exchange(requests, handler), handler::close)
                .doOnDiscard(ReferenceCounted.class, RELEASE)
                .handle(handler)
                .windowUntil(RESULT_DONE);
        });
    }

    /**
     * Execute multiple bindings of a client-preparing statement with one-by-one text query.
     * The execution terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}.
     * The {@link ErrorMessage} will emit an exception and cancel subsequent {@link Binding}s.
     *
     * @param client   the {@link Client} to exchange messages with.
     * @param query    the {@link TextQuery} for synthetic client-preparing statement.
     * @param bindings the data of bindings.
     * @return the messages received in response to this exchange, and will be completed
     * by {@link CompleteMessage} when it is last result for each binding.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, TextQuery query, List<Binding> bindings) {
        return Flux.defer(() -> {
            if (bindings.isEmpty()) {
                return Flux.empty();
            }

            EmitterProcessor<ClientMessage> requests = EmitterProcessor.create(1, false);
            TextQueryHandler handler = new TextQueryHandler(requests, query, bindings.iterator());

            return OperatorUtils.discardOnCancel(client.exchange(requests, handler), handler::close)
                .doOnDiscard(ReferenceCounted.class, RELEASE)
                .handle(handler)
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
        return multiQuery(client, InternalArrays.asIterator(statements))
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
                    return multiQuery(client, statements.iterator()).windowUntil(RESULT_DONE);
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
        return OperatorUtils.discardOnCancel(client.exchange(new SimpleQueryMessage(sql), FETCH_DONE))
            .doOnDiscard(ReferenceCounted.class, RELEASE)
            .handle((message, sink) -> {
                if (message instanceof ErrorMessage) {
                    sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                } else {
                    sink.next(message);
                }
            });
    }

    private static Flux<ServerMessage> multiQuery(Client client, Iterator<String> statements) {
        EmitterProcessor<ClientMessage> requests = EmitterProcessor.create(1, false);
        MultiQueryHandler handler = new MultiQueryHandler(requests, statements);

        return OperatorUtils.discardOnCancel(client.exchange(requests, handler), handler::close)
            .doOnDiscard(ReferenceCounted.class, RELEASE)
            .handle(handler);
    }

    private QueryFlow() {
    }
}

abstract class BaseHandler implements BiConsumer<ServerMessage, SynchronousSink<ServerMessage>>, Predicate<ServerMessage> {

    protected final EmitterProcessor<ClientMessage> requests;

    protected BaseHandler(EmitterProcessor<ClientMessage> requests) {
        this.requests = requests;
    }

    @Override
    public final boolean test(ServerMessage message) {
        if (message instanceof ErrorMessage) {
            return true;
        }

        if (!(message instanceof CompleteMessage) || !((CompleteMessage) message).isDone()) {
            return false;
        }

        if (!requests.isTerminated() && hasNext()) {
            requests.onNext(nextMessage());
            return false;
        } else {
            return true;
        }
    }

    abstract protected boolean hasNext();

    abstract protected ClientMessage nextMessage();
}

final class TextQueryHandler extends BaseHandler implements Consumer<String> {

    private final TextQuery query;

    private final Iterator<Binding> bindings;

    private String current;

    TextQueryHandler(EmitterProcessor<ClientMessage> requests, TextQuery query, Iterator<Binding> bindings) {
        super(requests);

        Binding binding = bindings.next();
        requests.onNext(binding.toTextMessage(query, this));
        this.query = query;
        this.bindings = bindings;
    }

    @Override
    public void accept(String s) {
        this.current = s;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(ExceptionFactory.createException((ErrorMessage) message, current));
        } else {
            sink.next(message);
        }
    }

    @Override
    protected boolean hasNext() {
        return bindings.hasNext();
    }

    @Override
    protected ClientMessage nextMessage() {
        return bindings.next().toTextMessage(query, this);
    }

    void close() {
        requests.onComplete();

        while (bindings.hasNext()) {
            bindings.next().clear();
        }
    }
}

final class MultiQueryHandler extends BaseHandler {

    private final Iterator<String> statements;

    private String current;

    MultiQueryHandler(EmitterProcessor<ClientMessage> requests, Iterator<String> statements) {
        super(requests);

        String current = statements.next();
        requests.onNext(new SimpleQueryMessage(current));
        this.current = current;
        this.statements = statements;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(ExceptionFactory.createException((ErrorMessage) message, current));
        } else {
            sink.next(message);
        }
    }

    @Override
    protected boolean hasNext() {
        return statements.hasNext();
    }

    @Override
    protected ClientMessage nextMessage() {
        String sql = statements.next();
        current = sql;
        return new SimpleQueryMessage(sql);
    }

    void close() {
        requests.onComplete();
    }
}

final class PrepareHandler implements BiConsumer<ServerMessage, SynchronousSink<ServerMessage>>, Predicate<ServerMessage> {

    private static final int PREPARE = 0;

    private static final int EXECUTE = 1;

    private static final int FETCH = 2;

    private final EmitterProcessor<ClientMessage> requests;

    private final String sql;

    private final Iterator<Binding> bindings;

    private final int fetchSize;

    private int mode = PREPARE;

    private PreparedOkMessage preparedOk;

    private PreparedFetchMessage fetch;

    PrepareHandler(EmitterProcessor<ClientMessage> requests, String sql, Iterator<Binding> bindings, int fetchSize) {
        requests.onNext(new PrepareQueryMessage(sql));

        this.requests = requests;
        this.sql = sql;
        this.bindings = bindings;
        this.fetchSize = fetchSize;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
            return;
        }

        switch (mode) {
            case PREPARE:
                // Ignore all messages in preparing phase.
                break;
            case EXECUTE:
                sink.next(message);
                break;
            default:
                if (message instanceof ServerStatusMessage) {
                    short statuses = ((ServerStatusMessage) message).getServerStatuses();
                    if ((statuses & ServerStatuses.LAST_ROW_SENT) != 0 || (statuses & ServerStatuses.CURSOR_EXISTS) == 0) {
                        sink.next(message);
                    }
                } else {
                    sink.next(message);
                }
                break;
        }
    }

    @Override
    public boolean test(ServerMessage message) {
        if (message instanceof ErrorMessage) {
            return true;
        }

        switch (mode) {
            case PREPARE:
                if (message instanceof PreparedOkMessage) {
                    PreparedOkMessage ok = (PreparedOkMessage) message;
                    int statementId = ok.getStatementId();
                    int columns = ok.getTotalColumns();
                    int parameters = ok.getTotalParameters();

                    preparedOk = ok;

                    // columns + parameters <= 0, has not metadata follow in,
                    if (columns <= -parameters) {
                        doNextExecute(statementId);
                    }
                } else if (message instanceof SyntheticMetadataMessage && ((SyntheticMetadataMessage) message).isCompleted()) {
                    doNextExecute(preparedOk.getStatementId());
                } else {
                    // This message will never be used.
                    ReferenceCountUtil.safeRelease(message);
                }

                return false;
            case EXECUTE:
                if (message instanceof CompleteMessage) {
                    // Complete message means execute phase done or fetch phase done (when cursor is not opened).
                    return fetchOrExecDone(message);
                } else if (message instanceof SyntheticMetadataMessage) {
                    EofMessage eof = ((SyntheticMetadataMessage) message).getEof();
                    if (eof instanceof ServerStatusMessage) {
                        // Non null.
                        if ((((ServerStatusMessage) eof).getServerStatuses() & ServerStatuses.CURSOR_EXISTS) != 0) {
                            doNextFetch();
                            return false;
                        }
                        // Otherwise means cursor does not be opened, wait for end of row EOF message.
                        return false;
                    }
                    // EOF is deprecated (null) or using EOF without statuses.
                    // EOF is deprecated: wait for OK message.
                    // EOF without statuses: means cursor does not be opened, wait for end of row EOF message.
                    return false;
                }

                return false;
            default:
                return fetchOrExecDone(message);
        }
    }

    void close() {
        if (!requests.isTerminated()) {
            if (preparedOk != null) {
                requests.onNext(new PreparedCloseMessage(preparedOk.getStatementId()));
            }
            requests.onComplete();
        }
        while (bindings.hasNext()) {
            bindings.next().clear();
        }
    }

    private void doNextExecute(int statementId) {
        Binding binding = bindings.next();

        mode = EXECUTE;
        requests.onNext(binding.toExecuteMessage(statementId, fetchSize <= 0));
    }

    private void doNextFetch() {
        mode = FETCH;
        requests.onNext(this.fetch == null ? (this.fetch = new PreparedFetchMessage(preparedOk.getStatementId(), fetchSize)) : this.fetch);
    }

    private boolean fetchOrExecDone(ServerMessage message) {
        if (!(message instanceof CompleteMessage) || !((CompleteMessage) message).isDone()) {
            return false;
        } else if (requests.isTerminated()) {
            return true;
        }

        if (message instanceof ServerStatusMessage) {
            short statuses = ((ServerStatusMessage) message).getServerStatuses();
            if ((statuses & ServerStatuses.CURSOR_EXISTS) != 0 && (statuses & ServerStatuses.LAST_ROW_SENT) == 0) {
                doNextFetch();
                return false;
            }
            // Otherwise is sent last row or never open cursor.
        }

        if (bindings.hasNext()) {
            doNextExecute(preparedOk.getStatementId());
            return false;
        } else {
            return true;
        }
    }
}

final class InitHandler implements BiConsumer<ServerMessage, SynchronousSink<Void>>, Predicate<ServerMessage> {

    private static final Logger logger = LoggerFactory.getLogger(InitHandler.class);

    private static final Map<String, String> ATTRIBUTES = Collections.emptyMap();

    private static final String CLI_SPECIFIC = "HY000";

    private static final int HANDSHAKE_VERSION = 10;

    private final EmitterProcessor<ClientMessage> requests;

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

    InitHandler(EmitterProcessor<ClientMessage> requests, Client client, SslMode sslMode, String database,
                String user, @Nullable CharSequence password, ConnectionContext context) {
        this.requests = requests;
        this.client = client;
        this.sslMode = sslMode;
        this.database = database;
        this.user = user;
        this.password = password;
        this.context = context;
        this.sslCompleted = sslMode == SslMode.TUNNEL;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<Void> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
            return;
        }

        if (handshake) {
            handshake = false;
            if (message instanceof HandshakeRequest) {
                int capabilities = initHandshake((HandshakeRequest) message);

                if ((capabilities & Capabilities.SSL) == 0) {
                    requests.onNext(createHandshakeResponse(capabilities));
                } else {
                    requests.onNext(SslRequest.from(capabilities, context.getClientCollation().getId()));
                }
            } else {
                sink.error(new R2dbcPermissionDeniedException("Unexpected message type '" +
                    message.getClass().getSimpleName() + "' in init phase"));
            }

            return;
        }

        if (message instanceof OkMessage) {
            requests.onComplete();
            client.loginSuccess();
        } else if (message instanceof SyntheticSslResponseMessage) {
            sslCompleted = true;
            requests.onNext(createHandshakeResponse(context.getCapabilities()));
        } else if (message instanceof AuthMoreDataMessage) {
            if (((AuthMoreDataMessage) message).isFailed()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Connection (id {}) fast authentication failed, auto-try to use full authentication", context.getConnectionId());
                }
                requests.onNext(createAuthResponse("full authentication"));
            }
            // Otherwise success, wait until OK message or Error message.
        } else if (message instanceof ChangeAuthMessage) {
            ChangeAuthMessage msg = (ChangeAuthMessage) message;
            authProvider = MySqlAuthProvider.build(msg.getAuthType());
            salt = msg.getSalt();
            requests.onNext(createAuthResponse("change authentication"));
        } else {
            sink.error(new R2dbcPermissionDeniedException("Unexpected message type '" +
                message.getClass().getSimpleName() + "' in login phase"));
        }
    }

    @Override
    public boolean test(ServerMessage message) {
        return message instanceof ErrorMessage || message instanceof OkMessage;
    }

    private AuthResponse createAuthResponse(String phase) {
        MySqlAuthProvider authProvider = getAndNextProvider();

        if (authProvider.isSslNecessary() && !sslCompleted) {
            throw new R2dbcPermissionDeniedException(formatAuthFails(authProvider.getType(), phase), CLI_SPECIFIC);
        }

        return new AuthResponse(authProvider.authentication(password, salt, context.getClientCollation()));
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
            logger.warn("The MySQL server use old handshake V{}, server version is {}, maybe most features are not available", handshakeVersion, serverVersion);
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

    private HandshakeResponse createHandshakeResponse(int capabilities) {
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

        return HandshakeResponse.from(capabilities, context.getClientCollation().getId(), user, authorization,
            authType, database, ATTRIBUTES);
    }

    private static String formatAuthFails(String authType, String phase) {
        return "Authentication type '" + authType + "' must require SSL in " + phase + " phase";
    }
}
