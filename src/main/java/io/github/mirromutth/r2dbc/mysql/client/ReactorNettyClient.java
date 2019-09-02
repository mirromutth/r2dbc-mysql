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

import io.github.mirromutth.r2dbc.mysql.MySqlSslConfiguration;
import io.github.mirromutth.r2dbc.mysql.internal.ConnectionContext;
import io.github.mirromutth.r2dbc.mysql.message.client.ClientMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.ExchangeableMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.ExitMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.SendOnlyMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
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
import reactor.netty.Connection;
import reactor.netty.FutureMono;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of client based on the Reactor Netty project.
 */
final class ReactorNettyClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private final Connection connection;

    private final EmitterProcessor<ServerMessage> responseProcessor = EmitterProcessor.create(false);

    private final ConnectionContext context;

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
            connection.addHandlerFirst(LoggingHandler.class.getSimpleName(), new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
        }

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
                    return it.doOnNext(message -> logger.debug("Response: {}", message));
                }

                return it;
            })
            .subscribe(this.responseProcessor::onNext, throwable -> {
                logger.error("Connection Error: {}", throwable.getMessage(), throwable);
                try {
                    responseProcessor.onError(throwable);
                } finally {
                    connection.dispose();
                }
            }, this.responseProcessor::onComplete);
    }

    @Override
    public Flux<ServerMessage> exchange(ExchangeableMessage request) {
        requireNonNull(request, "request must not be null");

        return Flux.defer(() -> {
            if (closing.get()) {
                return Flux.error(new IllegalStateException("cannot send messages because the connection is closed"));
            }

            send(request);

            return responseProcessor;
        });
    }

    @Override
    public Mono<Void> sendOnly(SendOnlyMessage message) {
        requireNonNull(message, "message must not be null");

        return Mono.defer(() -> {
            if (closing.get()) {
                return Mono.error(new IllegalStateException("cannot send messages because the connection is closed"));
            }

            return FutureMono.from(send(message));
        });
    }

    @Override
    public Mono<ServerMessage> nextMessage() {
        return Mono.defer(() -> {
            if (closing.get()) {
                return Mono.error(new IllegalStateException("cannot get messages because the connection is closed"));
            }

            return responseProcessor.next();
        });
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            if (!this.closing.compareAndSet(false, true)) {
                // client is closing or closed
                return Mono.empty();
            }

            // Should force any query which is processing and make sure send exit message.
            return FutureMono.from(send(ExitMessage.getInstance())).as(it -> {
                if (logger.isDebugEnabled()) {
                    return it.doOnSuccess(ignored -> logger.debug("Exit message has been sent successfully"));
                }
                return it;
            }).then(forceClose());
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
}
