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

import io.github.mirromutth.r2dbc.mysql.authentication.MySqlAuthProvider;
import io.github.mirromutth.r2dbc.mysql.client.Client;
import io.github.mirromutth.r2dbc.mysql.constant.AuthTypes;
import io.github.mirromutth.r2dbc.mysql.constant.Capabilities;
import io.github.mirromutth.r2dbc.mysql.constant.SqlStates;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.client.FullAuthResponse;
import io.github.mirromutth.r2dbc.mysql.message.client.HandshakeResponse;
import io.github.mirromutth.r2dbc.mysql.message.client.SslRequest;
import io.github.mirromutth.r2dbc.mysql.message.server.AuthChangeMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.AuthMoreDataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.HandshakeHeader;
import io.github.mirromutth.r2dbc.mysql.message.server.HandshakeRequest;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.SyntheticSslResponseMessage;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * A utility class that encapsulates the connection lifecycle phases flow.
 */
final class LoginFlow {

    private static final Logger logger = LoggerFactory.getLogger(LoginFlow.class);

    /**
     * Connection attributes, always empty for now.
     */
    private static final Map<String, String> attributes = Collections.emptyMap();

    private static final int CURRENT_HANDSHAKE_VERSION = 10;

    private final Client client;

    private final MySqlSession session;

    @Nullable
    private final Boolean requireSsl;

    private volatile boolean sslCompleted = false;

    private volatile MySqlAuthProvider authProvider;

    private volatile String username;

    private volatile CharSequence password;

    private volatile byte[] salt;

    private LoginFlow(Client client, MySqlSession session, String username, @Nullable CharSequence password, @Nullable Boolean requireSsl) {
        this.client = requireNonNull(client, "client must not be null");
        this.session = requireNonNull(session, "session must not be null");
        this.username = requireNonNull(username, "username must not be null");
        this.password = password;
        this.requireSsl = requireSsl;
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

        this.session.setConnectionId(header.getConnectionId());
        this.session.setServerVersion(serverVersion);
        this.session.setCapabilities(calculateClientCapabilities(message.getServerCapabilities()));

        this.authProvider = MySqlAuthProvider.build(message.getAuthType());
        this.salt = message.getSalt();
    }

    private boolean useSsl() {
        return (this.session.getCapabilities() & Capabilities.SSL) != 0;
    }

    private void changeAuth(AuthChangeMessage message) {
        this.authProvider = MySqlAuthProvider.build(message.getAuthType());
        this.salt = message.getSalt();
    }

    private SslRequest createSslRequest() {
        return SslRequest.from(session.getCapabilities(), session.getCollation().getId());
    }

    private MySqlAuthProvider getAndNextProvider() {
        MySqlAuthProvider authProvider = this.authProvider;
        this.authProvider = authProvider.next();
        return authProvider;
    }

    private Mono<HandshakeResponse> createHandshakeResponse() {
        return Mono.fromSupplier(() -> {
            MySqlAuthProvider authProvider = getAndNextProvider();

            String username = this.username;
            if (username == null) {
                throw new IllegalStateException("username must not be null when login");
            }

            byte[] authorization = authProvider.authentication(password, salt, session.getCollation());
            String authType = authProvider.getType();

            if (AuthTypes.NO_AUTH_PROVIDER.equals(authType)) {
                // Authentication type is not matter because of it has no authentication type.
                // Server need send a Authentication Change Request after handshake response.
                authType = AuthTypes.CACHING_SHA2_PASSWORD;
            }

            return HandshakeResponse.from(
                session.getCapabilities(),
                session.getCollation().getId(),
                username,
                authorization,
                authType,
                session.getDatabase(),
                attributes
            );
        });
    }

    private Mono<FullAuthResponse> createFullAuthResponse() {
        return Mono.fromSupplier(() -> {
            MySqlAuthProvider authProvider = getAndNextProvider();

            if (authProvider.isSslNecessary() && !sslCompleted) {
                throw new R2dbcPermissionDeniedException(String.format("Authentication type '%s' must require SSL in full authentication", authProvider.getType()), SqlStates.CLI_SPECIFIC_CONDITION);
            }

            return new FullAuthResponse(authProvider.authentication(password, salt, session.getCollation()));
        });
    }

    private int calculateClientCapabilities(int serverCapabilities) {
        if ((serverCapabilities & Capabilities.MULTI_STATEMENTS) == 0) {
            logger.warn("The MySQL server does not support batch executing, fallback to executing one-by-one");
        }

        // Server should always return metadata, and no compress, and without session track
        int clientCapabilities = serverCapabilities & ~(Capabilities.OPTIONAL_RESULT_SET_METADATA | Capabilities.COMPRESS | Capabilities.SESSION_TRACK);

        if ((clientCapabilities & Capabilities.SSL) == 0) {
            if (requireSsl != null) {
                if (requireSsl) {
                    throw new R2dbcPermissionDeniedException("Server unsupported SSL but user required SSL", SqlStates.CLI_SPECIFIC_CONDITION);
                } else {
                    client.sslUnsupported();
                }
            }
        } else if (requireSsl == null) {
            clientCapabilities &= ~(Capabilities.SSL | Capabilities.SSL_VERIFY_SERVER_CERT);
        }

        if (session.getDatabase().isEmpty() && (clientCapabilities & Capabilities.CONNECT_WITH_DB) != 0) {
            clientCapabilities &= ~Capabilities.CONNECT_WITH_DB;
        }

        if (attributes.isEmpty() && (clientCapabilities & Capabilities.CONNECT_ATTRS) != 0) {
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

    static Mono<Client> login(
        Client client, MySqlSession session,
        String username, @Nullable CharSequence password, @Nullable Boolean requireSsl
    ) {
        LoginFlow flow = new LoginFlow(client, session, username, password, requireSsl);
        EmitterProcessor<State> stateMachine = EmitterProcessor.create(true);

        return stateMachine.startWith(State.INIT)
            .handle((state, sink) -> {
                if (State.COMPLETED == state) {
                    sink.complete();
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Login state {} handling", state);
                    }
                    state.handle(flow).subscribe(stateMachine::onNext, stateMachine::onError);
                }
            }).doOnComplete(() -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Login succeed, cleanup intermediate variables");
                }
                flow.client.loginSuccess();
                flow.clearAuthentication();
            })
            .doOnError(e -> {
                flow.clearAuthentication();
                flow.client.forceClose().subscribe();
            }).then(Mono.just(client));
    }

    private enum State {

        INIT {
            @Override
            Mono<State> handle(LoginFlow flow) {
                return flow.client.readOnly().next().handle((message, sink) -> {
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
            @Override
            Mono<State> handle(LoginFlow flow) {
                return flow.client.exchange(Mono.just(flow.createSslRequest())).next().handle((message, sink) -> {
                    if (message instanceof ErrorMessage) {
                        sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
                    } else if (message instanceof SyntheticSslResponseMessage) {
                        flow.sslCompleted = true;
                        sink.next(HANDSHAKE);
                        sink.complete();
                    } else {
                        sink.error(new IllegalStateException(String.format("Unexpected message type '%s' in SSL handshake phase", message.getClass().getSimpleName())));
                    }
                });
            }
        },
        HANDSHAKE {
            @Override
            Mono<State> handle(LoginFlow flow) {
                return flow.createHandshakeResponse()
                    .flatMapMany(message -> flow.client.exchange(Mono.just(message)))
                    .<State>handle((message, sink) -> {
                        if (message instanceof ErrorMessage) {
                            sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
                        } else if (message instanceof OkMessage) {
                            sink.next(COMPLETED);
                            sink.complete();
                        } else if (message instanceof AuthMoreDataMessage) {
                            if (((AuthMoreDataMessage) message).getAuthMethodData()[0] != 3) {
                                if (logger.isInfoEnabled()) {
                                    logger.info("Connection (id {}) fast authentication failed, auto-try to use full authentication", flow.session.getConnectionId());
                                }
                                sink.next(FULL_AUTH);
                                sink.complete();
                            }
                            // Otherwise success, wait until OK message or Error message.
                        } else if (message instanceof AuthChangeMessage) {
                            flow.changeAuth((AuthChangeMessage) message);
                            sink.next(FULL_AUTH);
                            sink.complete();
                        } else {
                            sink.error(new IllegalStateException(String.format("Unexpected message type '%s' in handshake response phase", message.getClass().getSimpleName())));
                        }
                    }).next();
            }
        },
        /**
         * FULL_AUTH is also authentication change response phase.
         */
        FULL_AUTH {
            @Override
            Mono<State> handle(LoginFlow flow) {
                return flow.createFullAuthResponse()
                    .flatMap(response -> flow.client.exchange(Mono.just(response)).next())
                    .handle((message, sink) -> {
                        if (message instanceof ErrorMessage) {
                            sink.error(ExceptionFactory.createException((ErrorMessage) message, null));
                        } else if (message instanceof OkMessage) {
                            sink.next(COMPLETED);
                            sink.complete();
                        } else {
                            sink.error(new IllegalStateException(String.format("Unexpected message type '%s' in full authentication phase", message.getClass().getSimpleName())));
                        }
                    });
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
