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
import dev.miku.r2dbc.mysql.constant.AuthTypes;
import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.constant.DataValues;
import dev.miku.r2dbc.mysql.constant.SqlStates;
import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.message.client.AuthResponse;
import dev.miku.r2dbc.mysql.message.client.HandshakeResponse;
import dev.miku.r2dbc.mysql.message.client.SslRequest;
import dev.miku.r2dbc.mysql.message.server.AuthMoreDataMessage;
import dev.miku.r2dbc.mysql.message.server.ChangeAuthMessage;
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.HandshakeHeader;
import dev.miku.r2dbc.mysql.message.server.HandshakeRequest;
import dev.miku.r2dbc.mysql.message.server.OkMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticSslResponseMessage;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A utility class that encapsulates the connection lifecycle phases flow.
 */
final class LoginFlow {

    private static final Logger logger = LoggerFactory.getLogger(LoginFlow.class);

    /**
     * Connection attributes, always empty for now.
     */
    private static final Map<String, String> ATTRIBUTES = Collections.emptyMap();

    private static final int CURRENT_HANDSHAKE_VERSION = 10;

    private final Client client;

    private final ConnectionContext context;

    private final SslMode sslMode;

    private final String database;

    private volatile boolean sslCompleted = false;

    private volatile MySqlAuthProvider authProvider;

    private volatile String username;

    private volatile CharSequence password;

    private volatile byte[] salt;

    private LoginFlow(Client client, SslMode sslMode, String database, ConnectionContext context, String username, @Nullable CharSequence password) {
        this.client = requireNonNull(client, "client must not be null");
        this.sslMode = requireNonNull(sslMode, "sslMode must not be null");
        this.database = requireNonNull(database, "database must not be null");
        this.context = requireNonNull(context, "context must not be null");
        this.username = requireNonNull(username, "username must not be null");
        this.password = password;
    }

    /**
     * Other methods must be calling after this method, because capabilities should be initialized.
     */
    private void initHandshake(HandshakeRequest message) {
        HandshakeHeader header = message.getHeader();
        int handshakeVersion = header.getProtocolVersion();
        ServerVersion serverVersion = header.getServerVersion();

        if (handshakeVersion < CURRENT_HANDSHAKE_VERSION) {
            logger.warn("The MySQL server use old handshake V{}, server version is {}, maybe most features are not available", handshakeVersion, serverVersion);
        }

        // No need initialize server statuses because it has initialized by read filter.
        this.context.setConnectionId(header.getConnectionId());
        this.context.setServerVersion(serverVersion);
        this.context.setCapabilities(calculateClientCapabilities(message.getServerCapabilities()));

        this.authProvider = MySqlAuthProvider.build(message.getAuthType());
        this.salt = message.getSalt();
    }

    private boolean useSsl() {
        return (this.context.getCapabilities() & Capabilities.SSL) != 0;
    }

    private void changeAuth(ChangeAuthMessage message) {
        this.authProvider = MySqlAuthProvider.build(message.getAuthType());
        this.salt = message.getSalt();
    }

    private SslRequest createSslRequest() {
        return SslRequest.from(context.getCapabilities(), context.getClientCollation().getId());
    }

    private MySqlAuthProvider getAndNextProvider() {
        MySqlAuthProvider authProvider = this.authProvider;
        this.authProvider = authProvider.next();
        return authProvider;
    }

    private Mono<HandshakeResponse> createHandshakeResponse() {
        return Mono.fromSupplier(() -> {
            MySqlAuthProvider authProvider = getAndNextProvider();

            if (authProvider.isSslNecessary() && !sslCompleted) {
                throw new R2dbcPermissionDeniedException(formatAuthFails(authProvider.getType(), "handshake"), SqlStates.CLI_SPECIFIC_CONDITION);
            }

            String username = this.username;
            if (username == null) {
                throw new IllegalStateException("username must not be null when login");
            }

            byte[] authorization = authProvider.authentication(password, salt, context.getClientCollation());
            String authType = authProvider.getType();

            if (AuthTypes.NO_AUTH_PROVIDER.equals(authType)) {
                // Authentication type is not matter because of it has no authentication type.
                // Server need send a Change Authentication Message after handshake response.
                authType = AuthTypes.CACHING_SHA2_PASSWORD;
            }

            return HandshakeResponse.from(
                context.getCapabilities(),
                context.getClientCollation().getId(),
                username,
                authorization,
                authType,
                database,
                ATTRIBUTES
            );
        });
    }

    private Mono<AuthResponse> createAuthResponse(String phase) {
        return Mono.fromSupplier(() -> {
            MySqlAuthProvider authProvider = getAndNextProvider();

            if (authProvider.isSslNecessary() && !sslCompleted) {
                throw new R2dbcPermissionDeniedException(formatAuthFails(authProvider.getType(), phase), SqlStates.CLI_SPECIFIC_CONDITION);
            }

            return new AuthResponse(authProvider.authentication(password, salt, context.getClientCollation()));
        });
    }

    private int calculateClientCapabilities(int serverCapabilities) {
        // Remove unknown flags.
        int clientCapabilities = serverCapabilities & Capabilities.ALL_SUPPORTED;

        if ((clientCapabilities & Capabilities.SSL) == 0) {
            // Server unsupported SSL.
            if (sslMode.requireSsl()) {
                String message = String.format("Server version '%s' does not support SSL but mode '%s' requires SSL", context.getServerVersion(), sslMode);
                throw new R2dbcPermissionDeniedException(message, SqlStates.CLI_SPECIFIC_CONDITION);
            }

            if (sslMode.startSsl()) {
                // SSL has start yet, and client can be disable SSL, disable now.
                client.sslUnsupported();
            }
        } else {
            // Server supports SSL.
            if (!sslMode.startSsl()) {
                // SSL does not start, just remove flag.
                clientCapabilities &= ~Capabilities.SSL;
            }

            if (!sslMode.verifyCertificate()) {
                // No need verify server cert, remove flag.
                clientCapabilities &= ~Capabilities.SSL_VERIFY_SERVER_CERT;
            }
        }

        if (database.isEmpty() && (clientCapabilities & Capabilities.CONNECT_WITH_DB) != 0) {
            clientCapabilities &= ~Capabilities.CONNECT_WITH_DB;
        }

        if (ATTRIBUTES.isEmpty() && (clientCapabilities & Capabilities.CONNECT_ATTRS) != 0) {
            clientCapabilities &= ~Capabilities.CONNECT_ATTRS;
        }

        return clientCapabilities;
    }

    /**
     * All authentication data should be remove when connection phase completed or client closed in connection phase.
     */
    private void clearAuthentication() {
        this.username = null;
        this.password = null;
        this.salt = null;
        this.authProvider = null;
    }

    private void loginSuccess() {
        clearAuthentication();
        client.loginSuccess();
    }

    private void loginFailed() {
        clearAuthentication();
        client.forceClose().subscribe();
    }

    static Mono<Client> login(Client client, SslMode sslMode, String database, ConnectionContext context, String username, @Nullable CharSequence password) {
        LoginFlow flow = new LoginFlow(client, sslMode, database, context, username, password);
        EmitterProcessor<State> stateMachine = EmitterProcessor.create(1, true);

        stateMachine.onNext(State.INIT);

        Consumer<State> onStateNext = state -> {
            if (state == State.COMPLETED) {
                logger.debug("Login succeed, cleanup intermediate variables");
                flow.loginSuccess();
                stateMachine.onComplete();
            } else {
                stateMachine.onNext(state);
            }
        };
        Consumer<Throwable> onStateError = stateMachine::onError;

        return stateMachine.doOnNext(state -> {
            logger.debug("Login state {} handling", state);
            state.handle(flow).subscribe(onStateNext, onStateError);
        }).doOnError(ignored -> flow.loginFailed())
            .then(Mono.just(client));
    }

    private static String formatAuthFails(String authType, String phase) {
        return String.format("Authentication type '%s' must require SSL in %s phase", authType, phase);
    }

    private enum State {

        INIT {
            @Override
            Mono<State> handle(LoginFlow flow) {
                // Server send first, so no need send anything to server in here.
                return flow.client.receiveOnly().handle((message, sink) -> {
                    if (message instanceof ErrorMessage) {
                        sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
                    } else if (message instanceof HandshakeRequest) {
                        flow.initHandshake((HandshakeRequest) message);

                        if (flow.useSsl()) {
                            sink.next(SSL);
                            sink.complete();
                        } else {
                            sink.next(HANDSHAKE);
                            sink.complete();
                        }
                    } else {
                        sink.error(new IllegalStateException(String.format("Unexpected message type '%s' in handshake init phase", message.getClass().getSimpleName())));
                    }
                });
            }
        },
        SSL {

            private final Predicate<ServerMessage> complete = message -> message instanceof ErrorMessage || message instanceof SyntheticSslResponseMessage;

            @Override
            Mono<State> handle(LoginFlow flow) {
                return flow.client.exchange(flow.createSslRequest(), complete)
                    .<State>handle((message, sink) -> {
                        if (message instanceof ErrorMessage) {
                            sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
                        } else if (message instanceof SyntheticSslResponseMessage) {
                            flow.sslCompleted = true;
                            sink.next(HANDSHAKE);
                        } else {
                            sink.error(new IllegalStateException(String.format("Unexpected message type '%s' in SSL handshake phase", message.getClass().getSimpleName())));
                        }
                    })
                    .last();
            }
        },
        HANDSHAKE {

            private final Predicate<ServerMessage> complete = message -> message instanceof ErrorMessage || message instanceof OkMessage ||
                (message instanceof AuthMoreDataMessage && ((AuthMoreDataMessage) message).getAuthMethodData()[0] != DataValues.AUTH_SUCCEED) ||
                message instanceof ChangeAuthMessage;

            @Override
            Mono<State> handle(LoginFlow flow) {
                return flow.createHandshakeResponse()
                    .flatMapMany(message -> flow.client.exchange(message, complete))
                    .<State>handle((message, sink) -> {
                        if (message instanceof ErrorMessage) {
                            sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
                        } else if (message instanceof OkMessage) {
                            sink.next(COMPLETED);
                        } else if (message instanceof AuthMoreDataMessage) {
                            if (((AuthMoreDataMessage) message).getAuthMethodData()[0] != DataValues.AUTH_SUCCEED) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Connection (id {}) fast authentication failed, auto-try to use full authentication", flow.context.getConnectionId());
                                }
                                sink.next(FULL_AUTH);
                            }
                            // Otherwise success, wait until OK message or Error message.
                        } else if (message instanceof ChangeAuthMessage) {
                            flow.changeAuth((ChangeAuthMessage) message);
                            sink.next(CHANGE_AUTH);
                        } else {
                            sink.error(new IllegalStateException(String.format("Unexpected message type '%s' in handshake response phase", message.getClass().getSimpleName())));
                        }
                    })
                    .last();
            }
        },

        CHANGE_AUTH {

            private final Predicate<ServerMessage> complete = message -> message instanceof ErrorMessage || message instanceof OkMessage ||
                (message instanceof AuthMoreDataMessage && ((AuthMoreDataMessage) message).getAuthMethodData()[0] != DataValues.AUTH_SUCCEED);

            @Override
            Mono<State> handle(LoginFlow flow) {
                return flow.createAuthResponse("change authentication")
                    .flatMapMany(response -> flow.client.exchange(response, complete))
                    .<State>handle((message, sink) -> {
                        if (message instanceof ErrorMessage) {
                            sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
                        } else if (message instanceof OkMessage) {
                            sink.next(COMPLETED);
                        } else if (message instanceof AuthMoreDataMessage) {
                            if (((AuthMoreDataMessage) message).getAuthMethodData()[0] != DataValues.AUTH_SUCCEED) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Connection (id {}) fast authentication failed, auto-try to use full authentication", flow.context.getConnectionId());
                                }
                                sink.next(FULL_AUTH);
                            }
                            // Otherwise success, wait until OK message or Error message.
                        } else {
                            sink.error(new IllegalStateException(String.format("Unexpected message type '%s' in full authentication phase", message.getClass().getSimpleName())));
                        }
                    })
                    .last();
            }
        },

        FULL_AUTH {

            private final Predicate<ServerMessage> complete = message -> message instanceof ErrorMessage || message instanceof OkMessage;

            @Override
            Mono<State> handle(LoginFlow flow) {
                return flow.createAuthResponse("full authentication")
                    .flatMapMany(response -> flow.client.exchange(response, complete))
                    .<State>handle((message, sink) -> {
                        if (message instanceof ErrorMessage) {
                            sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
                        } else if (message instanceof OkMessage) {
                            sink.next(COMPLETED);
                        } else {
                            sink.error(new IllegalStateException(String.format("Unexpected message type '%s' in full authentication phase", message.getClass().getSimpleName())));
                        }
                    })
                    .last();
            }
        },
        COMPLETED {
            @Override
            Mono<State> handle(LoginFlow flow) {
                return Mono.just(COMPLETED);
            }
        };

        abstract Mono<State> handle(LoginFlow flow);
    }
}
