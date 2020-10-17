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

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.MySqlSslConfiguration;
import dev.miku.r2dbc.mysql.message.client.ClientMessage;
import dev.miku.r2dbc.mysql.message.client.ExitMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.WarningMessage;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.spi.R2dbcException;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.FutureMono;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of client based on the Reactor Netty project.
 */
final class ReactorNettyClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private static final boolean DEBUG_ENABLED = logger.isDebugEnabled();

    private static final boolean INFO_ENABLED = logger.isInfoEnabled();

    private static final Consumer<ReferenceCounted> RELEASE = ReferenceCounted::release;

    private final Connection connection;

    private final ConnectionContext context;

    private final EmitterProcessor<ClientMessage> requestProcessor = EmitterProcessor.create(false);

    private final EmitterProcessor<ServerMessage> responseProcessor = EmitterProcessor.create(false);

    private final RequestQueue requestQueue = new RequestQueue();

    private final AtomicBoolean closing = new AtomicBoolean();

    ReactorNettyClient(Connection connection, MySqlSslConfiguration ssl, ConnectionContext context) {
        requireNonNull(connection, "connection must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(ssl, "ssl must not be null");

        this.connection = connection;
        this.context = context;

        // Note: encoder/decoder should before reactor bridge.
        connection.addHandlerLast(EnvelopeSlicer.NAME, new EnvelopeSlicer())
            .addHandlerLast(MessageDuplexCodec.NAME, new MessageDuplexCodec(context, this.closing, this.requestQueue));

        if (ssl.getSslMode().startSsl()) {
            connection.addHandlerFirst(SslBridgeHandler.NAME, new SslBridgeHandler(context, ssl));
        }

        if (InternalLoggerFactory.getInstance(ReactorNettyClient.class).isTraceEnabled()) {
            // Or just use logger.isTraceEnabled()?
            logger.debug("Connection tracking logging is enabled");
            connection.addHandlerFirst(LoggingHandler.class.getSimpleName(), new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
        }

        ResponseSink sink = new ResponseSink();

        connection.inbound().receiveObject()
            .doOnNext(it -> {
                if (it instanceof ServerMessage) {
                    if (it instanceof ReferenceCounted) {
                        ((ReferenceCounted) it).retain();
                    }
                    sink.next((ServerMessage) it);
                } else {
                    // ReferenceCounted will released by Netty.
                    throw ClientExceptions.unsupportedProtocol(it.getClass().getTypeName());
                }
            })
            .onErrorResume(this::resumeError)
            .subscribe(new ResponseSubscriber(sink));

        this.requestProcessor.concatMap(message -> {
            if (DEBUG_ENABLED) {
                logger.debug("Request: {}", message);
            }

            return connection.outbound().sendObject(message);
        })
            .onErrorResume(this::resumeError)
            .doAfterTerminate(this::handleClose)
            .subscribe();
    }

    @Override
    public <T> Flux<T> exchange(ClientMessage request, BiConsumer<ServerMessage, SynchronousSink<T>> handler) {
        requireNonNull(request, "request must not be null");

        return Mono.<Flux<T>>create(sink -> {
            if (!isConnected()) {
                if (request instanceof Disposable) {
                    ((Disposable) request).dispose();
                }
                sink.error(ClientExceptions.exchangeClosed());
                return;
            }

            Flux<T> responses = OperatorUtils.discardOnCancel(responseProcessor
                .doOnSubscribe(ignored -> requestProcessor.onNext(request))
                .handle(handler)
                .doOnTerminate(requestQueue))
                .doOnDiscard(ReferenceCounted.class, RELEASE);

            requestQueue.submit(RequestTask.wrap(request, sink, responses));
        }).flatMapMany(identity());
    }

    @Override
    public <T> Flux<T> exchange(FluxExchangeable<T> exchangeable) {
        requireNonNull(exchangeable, "exchangeable must not be null");

        return Mono.<Flux<T>>create(sink -> {
            if (!isConnected()) {
                exchangeable.subscribe(request -> {
                    if (request instanceof Disposable) {
                        ((Disposable) request).dispose();
                    }
                }, requestProcessor::onError);
                sink.error(ClientExceptions.exchangeClosed());
                return;
            }

            Flux<T> responses = responseProcessor
                .doOnSubscribe(ignored -> exchangeable.subscribe(request -> {
                    if (isConnected()) {
                        requestProcessor.onNext(request);
                    } else {
                        if (request instanceof Disposable) {
                            ((Disposable) request).dispose();
                        }
                    }
                }, requestProcessor::onError))
                .handle(exchangeable)
                .doOnTerminate(() -> {
                    exchangeable.dispose();
                    requestQueue.run();
                });

            requestQueue.submit(RequestTask.wrap(exchangeable, sink, OperatorUtils.discardOnCancel(responses)
                .doOnDiscard(ReferenceCounted.class, RELEASE)
                .doOnCancel(exchangeable::dispose)));
        }).flatMapMany(identity());
    }

    @Override
    public Mono<Void> close() {
        return Mono.<Mono<Void>>create(sink -> {
            if (!closing.compareAndSet(false, true)) {
                // client is closing or closed
                sink.success();
                return;
            }

            requestQueue.submit(RequestTask.wrap(sink, Mono.fromRunnable(() -> requestProcessor.onNext(ExitMessage.INSTANCE))));
        }).flatMap(identity()).onErrorResume(e -> {
            logger.error("Exit message sending failed, force closing", e);
            return Mono.empty();
        }).then(forceClose());
    }

    @SuppressWarnings("unchecked")
    private <T> Mono<T> resumeError(Throwable e) {
        drainError(ClientExceptions.wrap(e));
        this.requestProcessor.onComplete();

        logger.error("Error: {}", e.getMessage(), e);

        return (Mono<T>) close();
    }

    @Override
    public Mono<Void> forceClose() {
        return FutureMono.deferFuture(() -> connection.channel().close());
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return connection.outbound().alloc();
    }

    @Override
    public boolean isConnected() {
        return !closing.get() && connection.channel().isOpen();
    }

    @Override
    public void sslUnsupported() {
        connection.channel().pipeline().fireUserEventTriggered(SslState.UNSUPPORTED);
    }

    @Override
    public void loginSuccess() {
        connection.channel().pipeline().fireUserEventTriggered(Lifecycle.COMMAND);
    }

    @Override
    public String toString() {
        return String.format("ReactorNettyClient(%s){connectionId=%d}", this.closing.get() ? "closing or closed" : "activating", context.getConnectionId());
    }

    private void drainError(R2dbcException e) {
        this.requestQueue.dispose();
        this.responseProcessor.onError(e);
    }

    private void handleClose() {
        if (this.closing.compareAndSet(false, true)) {
            logger.warn("Connection has been closed by peer");
            drainError(ClientExceptions.unexpectedClosed());
        } else {
            drainError(ClientExceptions.expectedClosed());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Function<T, T> identity() {
        return (Function<T, T>) Identity.INSTANCE;
    }

    private final class ResponseSubscriber implements CoreSubscriber<Object> {

        private final ResponseSink sink;

        private ResponseSubscriber(ResponseSink sink) {
            this.sink = sink;
        }

        @Override
        public Context currentContext() {
            return ReactorNettyClient.this.responseProcessor.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            ReactorNettyClient.this.responseProcessor.onSubscribe(s);
        }

        @Override
        public void onNext(Object message) {
        }

        @Override
        public void onError(Throwable t) {
            sink.error(t);
        }

        @Override
        public void onComplete() {
            handleClose();
        }
    }

    private final class ResponseSink implements SynchronousSink<ServerMessage> {

        @Override
        public void complete() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Context currentContext() {
            return ReactorNettyClient.this.responseProcessor.currentContext();
        }

        @Override
        public void error(Throwable e) {
            ReactorNettyClient.this.responseProcessor.onError(ClientExceptions.wrap(e));
        }

        @Override
        public void next(ServerMessage message) {
            if (message instanceof WarningMessage) {
                int warnings = ((WarningMessage) message).getWarnings();
                if (warnings == 0) {
                    if (DEBUG_ENABLED) {
                        logger.debug("Response: {}", message);
                    }
                } else if (INFO_ENABLED) {
                    logger.info("Response: {}, reports {} warning(s)", message, warnings);
                }
            } else if (DEBUG_ENABLED) {
                logger.debug("Response: {}", message);
            }

            ReactorNettyClient.this.responseProcessor.onNext(message);
        }
    }

    private static final class Identity implements Function<Object, Object> {

        private static final Identity INSTANCE = new Identity();

        @Override
        public Object apply(Object o) {
            return o;
        }
    }
}
