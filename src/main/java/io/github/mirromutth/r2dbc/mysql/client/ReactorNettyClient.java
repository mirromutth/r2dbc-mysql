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

package io.github.mirromutth.r2dbc.mysql.client;

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.Capabilities;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.core.ServerVersion;
import io.github.mirromutth.r2dbc.mysql.message.client.ClientMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.ExchangeableMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.ExitMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.HandshakeResponse41Message;
import io.github.mirromutth.r2dbc.mysql.message.server.AbstractHandshakeMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.AuthMoreDataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.HandshakeHeader;
import io.github.mirromutth.r2dbc.mysql.message.server.HandshakeV10Message;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.github.mirromutth.r2dbc.mysql.security.AuthStateMachine;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of client based on the Reactor Netty project.
 */
final class ReactorNettyClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private static final Consumer<ClientMessage> LOG_REQUEST = message -> logger.debug("Request: {}", message);

    private static final Consumer<ServerMessage> LOG_RESPONSE = message -> logger.debug("Response: {}", message);

    private static final Map<String, String> clientAttrs = calculateAttrs();

    private final Connection connection;

    private final EmitterProcessor<ServerMessage> responseProcessor = EmitterProcessor.create(false);

    private final MySqlSession session;

    private final AtomicBoolean closing = new AtomicBoolean();

    ReactorNettyClient(Connection connection, MySqlSession session) {
        requireNonNull(connection, "connection must not be null");
        requireNonNull(session, "session must not be null");

        this.connection = connection;
        this.session = session;

        // Note: encoder/decoder should before reactor bridge.
        connection.addHandlerLast(EnvelopeSlicer.NAME, new EnvelopeSlicer())
            .addHandlerLast(MessageDuplexCodec.NAME, new MessageDuplexCodec(session, this.closing));

        if (InternalLoggerFactory.getInstance(ReactorNettyClient.class).isTraceEnabled()) {
            // Or just use logger.isTraceEnabled()?
            connection.addHandlerFirst(LoggingHandler.class.getSimpleName(), new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
        }

        initReceive(connection);
    }

    @Override
    public Flux<ServerMessage> exchange(Publisher<? extends ExchangeableMessage> requests) {
        requireNonNull(requests, "request must not be null");

        return Flux.defer(() -> {
            if (this.closing.get()) {
                return Flux.error(new IllegalStateException("can not exchange messages because the connection is closed"));
            }

            return this.responseProcessor.doOnSubscribe(s -> send(requests).subscribe());
        });
    }

    @Override
    public Mono<Void> initialize() {
        return this.responseProcessor.<Void>handle((message, sink) -> {
            if (message instanceof ErrorMessage) {
                ErrorMessage msg = (ErrorMessage) message;
                sink.error(new R2dbcPermissionDeniedException(msg.getErrorMessage(), msg.getSqlState(), msg.getErrorCode()));
            } else if (message instanceof AbstractHandshakeMessage) {
                if (message instanceof HandshakeV10Message) {
                    initSession((HandshakeV10Message) message);
                    sink.complete();
                } else {
                    int handshakeVersion = ((AbstractHandshakeMessage) message).getHandshakeHeader().getProtocolVersion();
                    sink.error(new R2dbcPermissionDeniedException("unsupported handshake version " + handshakeVersion));
                }
            } else {
                sink.error(new R2dbcPermissionDeniedException("unknown message type '" + message.getClass().getSimpleName() + "' in handshake phase"));
            }
        })
            .thenMany(exchange(Mono.fromSupplier(this::createHandshakeResponse)))
            .<Void>handle((message, sink) -> {
                if (message instanceof ErrorMessage) {
                    ErrorMessage msg = (ErrorMessage) message;
                    sink.error(new R2dbcPermissionDeniedException(msg.getErrorMessage(), msg.getSqlState(), msg.getErrorCode()));
                } else if (message instanceof AuthMoreDataMessage) {
                    if (((AuthMoreDataMessage) message).getAuthMethodData()[0] != 3) {
                        if (this.session.isUseSsl()) {
                            if (logger.isWarnEnabled()) {
                                logger.warn("Connection (id {}) fast authentication failed, auto-try to use full authentication", this.session.getConnectionId());
                            }
                        } else {
                            close().doOnTerminate(() -> sink.error(new R2dbcPermissionDeniedException("Fast authentication failed and connection is not SSL, authentication all failed"))).subscribe();
                        }
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("Connection (id {}) fast authentication success", this.session.getConnectionId());
                    }
                } else if (message instanceof OkMessage) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Connection (id {}) authentication phase accepted", this.session.getConnectionId());
                    }
                    sink.complete();
                } else {
                    sink.error(new R2dbcPermissionDeniedException("unknown message type '" + message.getClass().getSimpleName() + "' in authentication phase"));
                }
            })
            .then();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            if (this.closing.getAndSet(true)) {
                // client is closing or closed
                return Mono.empty();
            }

            // Should force any query which is processing and make sure send exit message.
            return send(Mono.just(ExitMessage.getInstance()))
                .doOnSuccess(ignored -> connection.dispose())
                .then(connection.onDispose());
        });
    }

    private Mono<Void> send(Publisher<? extends ClientMessage> messages) {
        Publisher<? extends ClientMessage> requests;

        if (logger.isDebugEnabled()) {
            if (messages instanceof Mono) {
                @SuppressWarnings("unchecked")
                Mono<? extends ClientMessage> r = ((Mono<? extends ClientMessage>) messages);
                requests = r.doOnNext(LOG_REQUEST);
            } else {
                requests = Flux.from(messages).doOnNext(LOG_REQUEST);
            }
        } else {
            requests = messages;
        }

        return connection.outbound().sendObject(requests).then();
    }

    private void initReceive(Connection connection) {
        FluxSink<ServerMessage> responses = this.responseProcessor.sink();

        connection.inbound().receiveObject()
            .<ServerMessage>handle((msg, sink) -> {
                if (msg instanceof ServerMessage) {
                    if (msg instanceof ReferenceCounted) {
                        ((ReferenceCounted) msg).retain();
                    }
                    sink.next((ServerMessage) msg);
                } else {
                    // ReferenceCounted will released by Netty.
                    sink.error(new IllegalStateException("Impossible inbound type: " + msg.getClass()));
                }
            })
            .as(it -> {
                if (logger.isDebugEnabled()) {
                    return it.doOnNext(LOG_RESPONSE);
                }

                return it;
            })
            .doOnError(throwable -> {
                logger.error("Connection Error: {}", throwable.getMessage(), throwable);
                connection.dispose();
            })
            .subscribe(responses::next, responses::error, responses::complete);
    }

    private void initSession(HandshakeV10Message message) {
        HandshakeHeader header = message.getHandshakeHeader();
        ServerVersion version = header.getServerVersion();
        int serverCapabilities = message.getServerCapabilities();
        String authType = message.getAuthType();
        AuthStateMachine authStateMachine = AuthStateMachine.build(authType);
        CharCollation collation = CharCollation.defaultCollation(version);

//        if (authStateMachine.isSslNecessary()) {
//            this.session.setUseSsl(true);
//        }

        this.session.setConnectionId(header.getConnectionId());
        this.session.setServerVersion(version);
        this.session.setCollation(collation);
        this.session.setAuthStateMachine(authStateMachine);
        this.session.setSalt(message.getSalt());
        this.session.setServerCapabilities(serverCapabilities);
        this.session.setServerStatuses(message.getServerStatuses());

        int clientCapabilities = calculateClientCapabilities(serverCapabilities, this.session);

        this.session.setClientCapabilities(clientCapabilities);
    }

    private HandshakeResponse41Message createHandshakeResponse() {
        String username = session.getUsername();
        byte[] authentication = session.nextAuthentication();
        String authType = session.getAuthType();

        requireNonNull(username, "username must not be null at authentication phase");
        requireNonNull(authentication, "authentication must not be null at first authentication");
        requireNonNull(authType, "authType must not be null at authentication phase");

        // TODO: implement SSL

        return new HandshakeResponse41Message(
            session.getClientCapabilities(),
            session.getCollation().getId(),
            username,
            authentication,
            authType,
            session.getDatabase(),
            clientAttrs
        );
    }

    private static int calculateClientCapabilities(int serverCapabilities, MySqlSession session) {
        // use protocol 41 and deprecate EOF message
        int clientCapabilities = serverCapabilities | Capabilities.PROTOCOL_41 | Capabilities.DEPRECATE_EOF;

        // server should always return metadata
        // TODO: maybe need implement compress logic?
        clientCapabilities &= ~(Capabilities.OPTIONAL_RESULT_SET_METADATA | Capabilities.COMPRESS);

        if (session.isUseSsl()) {
            clientCapabilities |= Capabilities.SSL;
        } else {
            clientCapabilities &= ~(Capabilities.SSL | Capabilities.SSL_VERIFY_SERVER_CERT);
        }

        if (session.getDatabase().isEmpty()) {
            clientCapabilities &= ~Capabilities.CONNECT_WITH_DB;
        } else {
            clientCapabilities |= Capabilities.CONNECT_WITH_DB;
        }

        if (clientAttrs.isEmpty()) {
            clientCapabilities &= ~Capabilities.CONNECT_ATTRS;
        } else {
            clientCapabilities |= Capabilities.CONNECT_ATTRS;
        }

        return clientCapabilities;
    }

    private static Map<String, String> calculateAttrs() {
        // maybe write some OS or JVM message in here?
        return Collections.emptyMap();
    }
}
