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
import io.github.mirromutth.r2dbc.mysql.constant.AuthType;
import io.github.mirromutth.r2dbc.mysql.constant.Capability;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.core.ServerVersion;
import io.github.mirromutth.r2dbc.mysql.message.MessageCodec;
import io.github.mirromutth.r2dbc.mysql.message.backend.AbstractHandshakeMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.AuthMoreDataMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.CompleteMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.HandshakeHeader;
import io.github.mirromutth.r2dbc.mysql.message.backend.HandshakeV10Message;
import io.github.mirromutth.r2dbc.mysql.message.backend.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.ExitMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.HandshakeResponse41Message;
import io.netty.buffer.ByteBufUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.concurrent.Queues;
import reactor.util.concurrent.WaitStrategy;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
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

    private final EmitterProcessor<FrontendMessage> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<FrontendMessage> requests = requestProcessor.sink();

    private final Queue<MonoSink<Flux<BackendMessage>>> responseReceivers = Queues.<MonoSink<Flux<BackendMessage>>>unbounded().get();

    private final AtomicBoolean initialized = new AtomicBoolean();

    private volatile MySqlSession session;

    private volatile boolean closed = false;

    private ReactorNettyClient(
        Connection connection,
        ConnectProperties connectProperties,
        MonoProcessor<Void> handshakeProcessor
    ) {
        requireNonNull(connection, "connection must not be null");
        requireNonNull(handshakeProcessor, "handshakeProcessor must not be null");

        connection.addHandler(new EnvelopeDecoder())
            .addHandler(new SubscribersCompleteHandler(this.requestProcessor, this.responseReceivers));

        this.connection = new AtomicReference<>(connection);

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
                if (message instanceof HandshakeV10Message) {
                    HandshakeV10Message messageV10 = (HandshakeV10Message) message;
                    HandshakeHeader header = messageV10.getHandshakeHeader();
                    ServerVersion version = header.getServerVersion();
                    int serverCapabilities = messageV10.getServerCapabilities();

                    MySqlSession session = new MySqlSession(
                        header.getConnectionId(),
                        version,
                        serverCapabilities,
                        CharCollation.defaultCollation(version),
                        connectProperties.getDatabase(),
                        calculateCapabilities(serverCapabilities, connectProperties),
                        connectProperties.getUsername(),
                        connectProperties.getPassword(),
                        messageV10.getSalt(),
                        messageV10.getAuthType()
                    );

                    initSession(connection, session);
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

                boolean initialized = this.initialized.get();
                if (!initialized && this.initialized.compareAndSet(false, true)) {
                    session.clearAuthentication();
                    handshakeProcessor.onComplete();
                } else {
                    sink.next(message);
                }
            }
        );

        // TODO: fill missing handlers to receiver
        Mono<Void> receive = connection.inbound().receive()
            .retain()
            .doOnNext(buf -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("Inbound:\n{}", ByteBufUtil.prettyHexDump(buf));
                }
            })
            .concatMap(buf -> this.messageCodec.decode(buf, this.session))
            .doOnNext(message -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("Response: {}", message);
                }
            })
            .handle(handleHandshake)
            .handle(handleAuthMoreData)
            .handle(handleOk)
            .handle(handleError)
            .windowUntil(message -> message instanceof CompleteMessage)
            .doOnNext(messages -> {
                MonoSink<Flux<BackendMessage>> receiver = this.responseReceivers.poll();

                if (receiver != null) {
                    receiver.success(messages);
                }
            })
            .doOnComplete(() -> {
                MonoSink<Flux<BackendMessage>> receiver = this.responseReceivers.poll();

                if (receiver != null) {
                    receiver.success(Flux.empty());
                }
            })
            .then();

        Mono<Void> request = this.requestProcessor.doOnNext(message -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Request: {}", message);
            }
        }).concatMap(message -> send(connection, message)).then();

        Flux.merge(receive, request)
            .doFinally(ignored -> this.messageCodec.dispose())
            .onErrorResume(e -> {
                logger.error("Connection Error", e);
                return close();
            })
            .subscribe();
    }

    @Override
    public Flux<BackendMessage> exchange(FrontendMessage request) {
        requireNonNull(request, "request must not be null");

        return Mono.<Flux<BackendMessage>>create(sink -> {
            if (this.closed) {
                sink.error(new IllegalStateException("Cannot exchange messages because the connection is closed"));
                return;
            }

            if (request.isExchanged()) {
                synchronized (this) {
                    this.responseReceivers.add(sink);
                    this.requests.next(request);
                }
            } else {
                this.requests.next(request);
                sink.success(Flux.empty());
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

    private void initSession(Connection connection, MySqlSession session) {
        byte[] authentication = requireNonNull(session.nextAuthentication(), "authentication must not be null at first authentication");
        String username = requireNonNull(session.getUsername(), "username must not be null at authentication phase");
        AuthType authType = requireNonNull(session.getAuthType(), "authType must not be null at authentication phase");
        byte collationLow8Bits = (byte) (session.getCollation().getId() & 0xFF);

        this.session = session;

        FrontendMessage message = new HandshakeResponse41Message(
            session.getClientCapabilities(),
            collationLow8Bits,
            username,
            authentication,
            authType,
            session.getDatabase(),
            clientAttrs
        );

        send(connection, message).then().subscribe();
    }

    private NettyOutbound send(Connection connection, FrontendMessage message) {
        NettyOutbound outbound = connection.outbound();
        return outbound.send(messageCodec.encode(message, outbound.alloc(), this.session).doOnNext(buf -> {
            if (logger.isTraceEnabled()) {
                logger.trace("Outbound:\n{}", ByteBufUtil.prettyHexDump(buf));
            }
        }));
    }

    static Mono<ReactorNettyClient> connect(ConnectProperties connectProperties) {
        return connect(ConnectionProvider.newConnection(), connectProperties);
    }

    private static Mono<ReactorNettyClient> connect(ConnectionProvider connectionProvider, ConnectProperties connectProperties) {
        requireNonNull(connectionProvider, "connectionProvider must not be null");
        requireNonNull(connectProperties, "connectProperties must not be null");

        TcpClient client = TcpClient.create(connectionProvider)
            .host(connectProperties.getHost())
            .port(connectProperties.getPort());

        MonoProcessor<Void> handshake = MonoProcessor.create(WaitStrategy.sleeping());

        return client.connect()
            .map(conn -> new ReactorNettyClient(conn, connectProperties, handshake))
            .flatMap(c -> handshake.then(Mono.just(c)));
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

    private static int calculateCapabilities(int serverCapabilities, ConnectProperties properties) {
        // use protocol 41 and deprecate EOF message
        int clientCapabilities = serverCapabilities | Capability.PROTOCOL_41.getFlag() | Capability.DEPRECATE_EOF.getFlag();

        // server should always return metadata
        clientCapabilities &= ~Capability.OPTIONAL_RESULT_SET_METADATA.getFlag();
        // TODO: maybe need implement compress logic?
        clientCapabilities &= ~Capability.COMPRESS.getFlag();

        if (properties.isUseSsl()) {
            clientCapabilities |= Capability.SSL.getFlag();
        } else {
            clientCapabilities &= ~Capability.SSL.getFlag();
            clientCapabilities &= ~Capability.SSL_VERIFY_SERVER_CERT.getFlag();
        }

        if (properties.getDatabase().isEmpty()) {
            clientCapabilities &= ~Capability.CONNECT_WITH_DB.getFlag();
        } else {
            clientCapabilities |= Capability.CONNECT_WITH_DB.getFlag();
        }

        if (clientAttrs.isEmpty()) {
            clientCapabilities &= ~Capability.CONNECT_ATTRS.getFlag();
        } else {
            clientCapabilities |= Capability.CONNECT_ATTRS.getFlag();
        }

        return clientCapabilities;
    }

    private static Map<String, String> calculateAttrs() {
        // maybe write some OS or JVM message in here?
        return Collections.emptyMap();
    }
}
