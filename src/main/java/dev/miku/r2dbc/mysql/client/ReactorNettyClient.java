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

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.MySqlSslConfiguration;
import dev.miku.r2dbc.mysql.internal.ConnectionContext;
import dev.miku.r2dbc.mysql.message.client.ClientMessage;
import dev.miku.r2dbc.mysql.message.client.ExchangeableMessage;
import dev.miku.r2dbc.mysql.message.client.ExitMessage;
import dev.miku.r2dbc.mysql.message.client.SendOnlyMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.WarningMessage;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.FutureMono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of client based on the Reactor Netty project.
 */
final class ReactorNettyClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private static final Function<Flux<ServerMessage>, Flux<ServerMessage>> IDENTITY = Function.identity();

    private static final Consumer<ServerMessage> INFO_LOGGING = ReactorNettyClient::infoLogging;

    private static final Consumer<ServerMessage> DEBUG_LOGGING = message -> {
        logger.debug("Response: {}", message);
        infoLogging(message);
    };

    private static final BiConsumer<Object, SynchronousSink<ServerMessage>> INBOUND_HANDLE =
        ReactorNettyClient::inboundHandle;

    private final Connection connection;

    private final ConnectionContext context;

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
            .addHandlerLast(MessageDuplexCodec.NAME, new MessageDuplexCodec(context, this.closing));

        if (ssl.getSslMode().startSsl()) {
            connection.addHandlerFirst(SslBridgeHandler.NAME, new SslBridgeHandler(context, ssl));
        }

        if (InternalLoggerFactory.getInstance(ReactorNettyClient.class).isTraceEnabled()) {
            // Or just use logger.isTraceEnabled()?
            logger.debug("Connection tracking logging is enabled");
            connection.addHandlerFirst(LoggingHandler.class.getSimpleName(), new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
        }

        Flux<ServerMessage> inbound = connection.inbound().receiveObject()
            .handle(INBOUND_HANDLE);

        if (logger.isDebugEnabled()) {
            inbound = inbound.doOnNext(DEBUG_LOGGING);
        } else if (logger.isInfoEnabled()) {
            inbound = inbound.doOnNext(INFO_LOGGING);
        }

        inbound.subscribe(this.responseProcessor::onNext, throwable -> {
            try {
                logger.error("Connection Error: {}", throwable.getMessage(), throwable);
                responseProcessor.onError(throwable);
            } finally {
                connection.dispose();
            }
        }, this.responseProcessor::onComplete);
    }

    @Override
    public Flux<ServerMessage> exchange(ExchangeableMessage request, Predicate<ServerMessage> complete) {
        requireNonNull(request, "request must not be null");

        boolean[] completed = new boolean[]{false};

        return Mono.<Flux<ServerMessage>>create(sink -> {
            if (closing.get()) {
                sink.error(new IllegalStateException("Cannot send messages because the connection is closed"));
                return;
            }

            requestQueue.submit(() -> {
                send(request);
                sink.success(responseProcessor);
            });
        })
            .flatMapMany(IDENTITY)
            .<ServerMessage>handle((message, sink) -> {
                sink.next(message);

                if (complete.test(message)) {
                    completed[0] = true;
                    sink.complete();
                }
            })
            .doOnTerminate(requestQueue)
            .doOnCancel(exchangeCancel(completed));
    }

    @Override
    public Mono<Void> sendOnly(SendOnlyMessage message) {
        requireNonNull(message, "message must not be null");

        return Mono.<Flux<ServerMessage>>create(sink -> {
            if (closing.get()) {
                sink.error(new IllegalStateException("Cannot send messages because the connection is closed"));
                return;
            }

            requestQueue.submit(() -> {
                send(message);
                sink.success(Flux.empty());
            });
        })
            .flatMapMany(IDENTITY)
            .doOnTerminate(requestQueue)
            .then();
    }

    @Override
    public Mono<ServerMessage> nextMessage() {
        boolean[] completed = new boolean[]{false};

        return Mono.<Flux<ServerMessage>>create(sink -> {
            if (closing.get()) {
                sink.error(new IllegalStateException("Cannot send messages because the connection is closed"));
                return;
            }

            requestQueue.submit(() -> sink.success(responseProcessor));
        })
            .flatMapMany(IDENTITY)
            .<ServerMessage>handle((message, sink) -> {
                sink.next(message);
                completed[0] = true;
                sink.complete();
            })
            .doOnTerminate(requestQueue)
            .doOnCancel(exchangeCancel(completed))
            .last();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            if (!this.closing.compareAndSet(false, true)) {
                // client is closing or closed
                return Mono.empty();
            }

            // Should force any query which is processing and make sure send exit message.
            Mono<Void> closer = FutureMono.from(send(ExitMessage.getInstance()));

            if (logger.isDebugEnabled()) {
                closer = closer.doOnSuccess(ignored -> logger.debug("Exit message has been sent successfully"));
            }

            return closer.then(forceClose());
        });
    }

    @Override
    public Mono<Void> forceClose() {
        return FutureMono.deferFuture(() -> connection.channel().close());
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

    private Future<Void> send(ClientMessage message) {
        logger.debug("Request: {}", message);
        return connection.channel().writeAndFlush(message);
    }

    private static void inboundHandle(Object msg, SynchronousSink<ServerMessage> sink) {
        if (msg instanceof ServerMessage) {
            if (msg instanceof ReferenceCounted) {
                ((ReferenceCounted) msg).retain();
            }
            sink.next((ServerMessage) msg);
        } else {
            // ReferenceCounted will released by Netty.
            sink.error(new IllegalStateException("Impossible inbound type: " + msg.getClass()));
        }
    }

    private static Runnable exchangeCancel(boolean[] completed) {
        return () -> {
            if (!completed[0]) {
                logger.error("Exchange cancelled while exchange is active. This is likely a bug leading to unpredictable outcome.");
            }
        };
    }

    private static void infoLogging(ServerMessage message) {
        if (message instanceof WarningMessage) {
            int warnings = ((WarningMessage) message).getWarnings();
            if (warnings != 0) {
                logger.info("MySQL reports {} warning(s)", warnings);
            }
        }
    }
}
