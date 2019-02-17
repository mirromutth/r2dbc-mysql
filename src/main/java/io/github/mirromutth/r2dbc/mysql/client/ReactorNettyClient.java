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

import io.github.mirromutth.r2dbc.mysql.constant.DecodeMode;
import io.github.mirromutth.r2dbc.mysql.core.CharCollation;
import io.github.mirromutth.r2dbc.mysql.core.ServerSession;
import io.github.mirromutth.r2dbc.mysql.message.backend.AbstractHandshakeMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessageDecoder;
import io.github.mirromutth.r2dbc.mysql.message.backend.HandshakeHeader;
import io.github.mirromutth.r2dbc.mysql.message.backend.HandshakeV10Message;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.netty.buffer.ByteBufUtil;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.util.concurrent.Queues;
import reactor.util.concurrent.WaitStrategy;

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

    private final AtomicReference<Connection> connection;

    private final BackendMessageDecoder messageDecoder;

    private final MonoProcessor<ServerSession> serverSession = MonoProcessor.create(WaitStrategy.sleeping());

    private final EmitterProcessor<FrontendMessage> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<FrontendMessage> requests = requestProcessor.sink();

    private final Queue<ResponseReceiver> responseReceivers = Queues.<ResponseReceiver>unbounded().get();

    private volatile boolean handshakeHandled = false;

    private volatile boolean closed = false;

    ReactorNettyClient(Connection connection) {
        requireNonNull(connection, "connection must not be null");

        connection.addHandler(new SubscribersCompleteHandler(this.requestProcessor, this.responseReceivers));

        this.messageDecoder = new BackendMessageDecoder(connection.outbound().alloc());
        this.connection = new AtomicReference<>(connection);

        BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleHandshake = handleBackendMessage(
            AbstractHandshakeMessage.class,
            (message, sink) -> {
                this.handshakeHandled = true;
                if (message instanceof HandshakeV10Message) {
                    HandshakeV10Message messageV10 = (HandshakeV10Message) message;
                    HandshakeHeader header = messageV10.getHandshakeHeader();

                    // TODO: use full index, not just lower 8-bits
                    ServerSession session = new ServerSession(
                        header.getConnectionId(),
                        header.getServerVersion(),
                        messageV10.getServerCapabilities(),
                        messageV10.getAuthType(),
                        CharCollation.fromId(messageV10.getCollationLow8Bits())
                    );

                    this.messageDecoder.setServerSession(session);
                    this.serverSession.onNext(session);
                } else {
                    forceClose().subscribe();
                }
            }
        );

        // TODO: implement receive, maybe need concat packets before doOnNext?
        Mono<Void> receive = connection.inbound().receive()
            .retain()
            .doOnNext(byteBuf -> {
                if (!this.handshakeHandled) {
                    this.messageDecoder.decode(byteBuf, DecodeMode.HANDSHAKE).handle(handleHandshake).subscribe();
                    return;
                }

                ResponseReceiver receiver = this.responseReceivers.poll();

                if (receiver != null) {
                    receiver.success(this.messageDecoder.decode(byteBuf, receiver.getDecodeMode()));
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unhandled byte buffer: {}", ByteBufUtil.prettyHexDump(byteBuf));
                    }
                }
            })
            .doOnComplete(() -> {
                ResponseReceiver receiver = this.responseReceivers.poll();

                if (receiver != null) {
                    receiver.success(Flux.empty());
                }
            })
            .then();

        Mono<Void> request = this.requestProcessor.doOnNext(message -> logger.debug("Request: {}", message))
            .concatMap(message -> connection.outbound().send(message.encode(connection.outbound().alloc(), requireNonNull(serverSession.peek(), "send request before handshake complete"))))
            .then();

        Flux.merge(receive, request)
            .onErrorResume(throwable -> {
                logger.error("Connection Error", throwable);
                return close();
            })
            .doFinally(s -> messageDecoder.release())
            .subscribe();
    }

    @Override
    public Flux<BackendMessage> exchange(Publisher<FrontendMessage> requests) {
        requireNonNull(requests, "requests must not be null");

        return Mono.<Flux<BackendMessage>>create(sink -> {
            if (this.closed) {
                sink.error(new IllegalStateException("Cannot exchange messages because the connection is closed"));
            }

            final AtomicBoolean once = new AtomicBoolean();

            Flux.from(requests).subscribe(message -> {
                if (!once.get() && once.compareAndSet(false, true)) {
                    synchronized (this) {
                        DecodeMode mode = message.responseDecodeMode();

                        // no need append to response receivers if it has no response
                        if (mode != null) {
                            this.responseReceivers.add(new ResponseReceiver(sink, mode));
                        }
                    }
                }

                this.requests.next(message);
            }, this.requests::error);
        }).flatMapMany(Function.identity());
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            Connection connection = this.connection.getAndSet(null);

            if (connection == null) { // client is closed or closing
                return Mono.empty();
            }

            // TODO: implement close

            return Mono.empty();
        });
    }

    private Mono<Void> forceClose() {
        return Mono.defer(() -> {
            Connection connection = this.connection.getAndSet(null);

            if (connection == null) { // client is closed or closing
                return Mono.empty();
            }

            connection.dispose();

            return connection.onDispose().doOnSuccess(v -> this.closed = true);
        });
    }

    @Override
    public Mono<ServerSession> getSession() {
        return serverSession;
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
}
