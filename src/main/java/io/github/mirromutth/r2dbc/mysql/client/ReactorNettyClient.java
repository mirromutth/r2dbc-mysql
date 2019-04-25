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
import io.github.mirromutth.r2dbc.mysql.config.ConnectProperties;
import io.github.mirromutth.r2dbc.mysql.constant.Capabilities;
import io.github.mirromutth.r2dbc.mysql.constant.ProtocolPhase;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.core.ServerVersion;
import io.github.mirromutth.r2dbc.mysql.message.MessageCodec;
import io.github.mirromutth.r2dbc.mysql.message.backend.AbstractHandshakeMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.AuthMoreDataMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.CompleteMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.DecodeContext;
import io.github.mirromutth.r2dbc.mysql.message.backend.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.HandshakeHeader;
import io.github.mirromutth.r2dbc.mysql.message.backend.HandshakeV10Message;
import io.github.mirromutth.r2dbc.mysql.message.backend.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.ExitMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.HandshakeResponse41Message;
import io.github.mirromutth.r2dbc.mysql.security.AuthStateMachine;
import io.netty.buffer.ByteBufUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of client based on the Reactor Netty project.
 */
final class ReactorNettyClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private static final Map<String, String> clientAttrs = calculateAttrs();

    private final AtomicReference<Connection> connection;

    private final MessageCodec messageCodec = new MessageCodec();

    private final AtomicReference<MessageContext> processing = new AtomicReference<>();

    private final AtomicBoolean initialized = new AtomicBoolean();

    private final MySqlSession session;

    private volatile boolean closed = false;

    private ReactorNettyClient(
        Connection connection,
        ConnectProperties connectProperties,
        MonoSink<ReactorNettyClient> handshakeSink
    ) {
        requireNonNull(handshakeSink, "handshakeSink must not be null");
        // try NOT use connectProperties on lambda, because it includes username and password, lambda maybe holding it.
        requireNonNull(connectProperties, "connectProperties must not be null");

        this.session = new MySqlSession(
            connectProperties.isUseSsl(),
            connectProperties.getDatabase(),
            connectProperties.getUsername(),
            connectProperties.getPassword()
        );

        this.connection = new AtomicReference<>(requireNonNull(connection, "connection must not be null"));

        connection.addHandler(new EnvelopeDecoder())
            .addHandler(new SubscribersCompleteHandler(this.processing));

        BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleError = handleBackendMessage(
            ErrorMessage.class,
            (message, sink) -> {
                logger.error("Error: error code {}, sql state: {}, message: {}", message.getErrorCode(), message.getSqlState(), message.getErrorMessage());
                sink.next(message);
            }
        );

        BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleHandshake = handleBackendMessage(
            AbstractHandshakeMessage.class,
            (message, sink) -> {
                Connection conn = this.connection.get();

                if ((message instanceof HandshakeV10Message) && conn != null) {
                    initSession(conn, (HandshakeV10Message) message);
                } else {
                    close().subscribe();
                }
            }
        );

        BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleAuthMoreData = handleBackendMessage(
            AuthMoreDataMessage.class,
            (message, sink) -> {
                switch (message.getAuthMethodData()[0]) {
                    case 3: // success, ignored
                        break;
                    case 4:
                        session.nextAuthentication(); // TODO: implement full authentication
                        break;
                }
            }
        );

        BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleOk = handleBackendMessage(
            OkMessage.class,
            (message, sink) -> {
                session.setServerStatuses(message.getServerStatuses());

                boolean initialized = this.initialized.getAndSet(true);

                if (initialized) {
                    sink.next(message);
                } else {
                    session.clearAuthentication();
                    messageCodec.changePhase(ProtocolPhase.COMMAND);
                    handshakeSink.success(this);
                }
            }
        );

        initReceive(connection, handleError, handleHandshake, handleAuthMoreData, handleOk);
    }

    @Override
    public Flux<BackendMessage> exchange(FrontendMessage request) {
        requireNonNull(request, "request must not be null");

        return Mono.<Flux<BackendMessage>>create(sink -> {
            Connection connection = this.connection.get();

            if (this.closed || connection == null) {
                sink.error(new IllegalStateException("connection is closing or closed"));
                return;
            }

            MessageContext context = new MessageContext(request, sink);

            if (this.processing.compareAndSet(null, context)) {
                if (request.isExchanged()) {
                    context.initDecodeContext();
                    send(connection, request).then().subscribe();
                } else {
                    send(connection, request).then().doOnTerminate(() -> {
                        this.processing.compareAndSet(context, null);
                        context.success(Flux.empty());
                    }).subscribe();
                }
            } else {
                sink.error(new IllegalStateException("connection support only single querying in the moment"));
            }
        }).flatMapMany(Function.identity());
    }

    @Override
    public int getConnectionId() {
        return session.getConnectionId();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            Connection connection = this.connection.getAndSet(null);

            if (connection == null) { // client is closed or closing
                return Mono.empty();
            }

            return Mono.just(ExitMessage.getInstance())
                .doOnNext(message -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Request: {}", message);
                    }
                })
                .flatMapMany(message -> send(connection, message))
                .then()
                .doOnSuccess(ignored -> connection.dispose())
                .then(connection.onDispose())
                .doOnSuccess(ignored -> this.closed = true);
        });
    }

    @SafeVarargs
    private final void initReceive(Connection connection, BiConsumer<BackendMessage, SynchronousSink<BackendMessage>>... handlers) {
        Flux<BackendMessage> receive = connection.inbound().receive()
            .retain()
            .doOnNext(buf -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("Inbound:\n{}", ByteBufUtil.prettyHexDump(buf));
                }
            })
            .concatMap(buf -> this.messageCodec.decode(buf, processingContext(), this.session))
            .doOnNext(message -> logger.debug("Response: {}", message));

        for (BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handler : handlers) {
            receive = receive.handle(handler);
        }

        receive.windowUntil(message -> message instanceof CompleteMessage)
            .doOnNext(messages -> {
                MessageContext context = this.processing.getAndSet(null);

                if (context != null) {
                    context.success(messages);
                }
            })
            .doOnComplete(() -> {
                MessageContext context = this.processing.getAndSet(null);

                if (context != null) {
                    context.success(Flux.empty());
                }
            })
            .then()
            .doFinally(s -> {
                MessageContext context = this.processing.getAndSet(null);

                if (context != null) {
                    context.error(new IllegalStateException("receiver is disposed"));
                }

                this.messageCodec.dispose();
            })
            .onErrorResume(throwable -> {
                logger.error("Connection Error", throwable);
                return close();
            })
            .subscribe();
    }

    private void initSession(Connection connection, HandshakeV10Message message) {
        HandshakeHeader header = message.getHandshakeHeader();
        ServerVersion version = header.getServerVersion();
        int serverCapabilities = message.getServerCapabilities();
        String authType = message.getAuthType();
        AuthStateMachine authStateMachine = AuthStateMachine.build(authType);
        CharCollation collation = CharCollation.defaultCollation(version);

        this.session.setConnectionId(header.getConnectionId());
        this.session.setServerVersion(version);
        this.session.setCollation(collation);
        this.session.setAuthStateMachine(authStateMachine);
        this.session.setSalt(message.getSalt());
        this.session.setServerCapabilities(serverCapabilities);
        this.session.setServerStatuses(message.getServerStatuses());

        int clientCapabilities = calculateClientCapabilities(serverCapabilities, this.session);

        this.session.setClientCapabilities(clientCapabilities);

        send(connection, new HandshakeResponse41Message(
            clientCapabilities,
            collation.getId(),
            requireNonNull(session.getUsername(), "username must not be null at authentication phase"),
            requireNonNull(session.nextAuthentication(), "authentication must not be null at first authentication"),
            authType,
            session.getDatabase(),
            clientAttrs
        )).then().subscribe();
    }

    private NettyOutbound send(Connection connection, FrontendMessage message) {
        NettyOutbound outbound = connection.outbound();
        return outbound.send(messageCodec.encode(message, outbound.alloc(), this.session).doOnNext(buf -> {
            if (logger.isTraceEnabled()) {
                logger.trace("Outbound:\n{}", ByteBufUtil.prettyHexDump(buf));
            }
        }));
    }

    private DecodeContext processingContext() {
        MessageContext context = this.processing.get();

        if (context == null) {
            return DecodeContext.normal();
        }

        return context.getDecodeContext();
    }

    static Mono<ReactorNettyClient> connect(ConnectProperties connectProperties) {
        return connect(ConnectionProvider.newConnection(), connectProperties);
    }

    private static Mono<ReactorNettyClient> connect(
        ConnectionProvider connectionProvider,
        ConnectProperties connectProperties
    ) {
        requireNonNull(connectionProvider, "connectionProvider must not be null");
        requireNonNull(connectProperties, "connectProperties must not be null");

        return Mono.create(sink -> TcpClient.create(connectionProvider)
            .host(connectProperties.getHost())
            .port(connectProperties.getPort())
            .connect()
            .map(conn -> new ReactorNettyClient(conn, connectProperties, sink))
        );
    }

    @SuppressWarnings("unchecked")
    private static <T extends BackendMessage> BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleBackendMessage(
        Class<T> type,
        BiConsumer<T, SynchronousSink<BackendMessage>> consumer
    ) {
        return (message, sink) -> {
            if (type.isInstance(message)) {
                consumer.accept((T) message, sink);
            } else {
                sink.next(message);
            }
        };
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
