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
import io.github.mirromutth.r2dbc.mysql.constant.SslMode;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.client.ClientMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.ExchangeableMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.ExitMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.SendOnlyMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.FutureMono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of client based on the Reactor Netty project.
 */
final class ReactorNettyClient implements Client {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private static final Consumer<ClientMessage> LOG_REQUEST = message -> logger.debug("Request: {}", message);

    private static final Consumer<ServerMessage> LOG_RESPONSE = message -> logger.debug("Response: {}", message);

    private final Connection connection;

    private final EmitterProcessor<ServerMessage> responseProcessor = EmitterProcessor.create(false);

    private final MySqlSession session;

    private final AtomicBoolean closing = new AtomicBoolean();

    ReactorNettyClient(Connection connection, MySqlSslConfiguration ssl, MySqlSession session) {
        requireNonNull(connection, "connection must not be null");
        requireNonNull(session, "session must not be null");
        requireNonNull(ssl, "ssl must not be null");

        this.connection = connection;
        this.session = session;

        // Note: encoder/decoder should before reactor bridge.
        connection.addHandlerLast(EnvelopeSlicer.NAME, new EnvelopeSlicer())
            .addHandlerLast(MessageDuplexCodec.NAME, new MessageDuplexCodec(session, this.closing));

        if (ssl.getSslMode().startSsl()) {
            connection.addHandlerFirst(SslBridgeHandler.NAME, new SslBridgeHandler(session, ssl));
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
                    return it.doOnNext(LOG_RESPONSE);
                }

                return it;
            })
            .doOnError(throwable -> {
                logger.error("Connection Error: {}", throwable.getMessage(), throwable);
                connection.dispose();
            })
            .subscribe(this.responseProcessor::onNext, this.responseProcessor::onError, this.responseProcessor::onComplete);
    }

    @Override
    public Flux<ServerMessage> exchange(Publisher<? extends ExchangeableMessage> requests) {
        requireNonNull(requests, "requests must not be null");

        return Flux.defer(() -> {
            if (this.closing.get()) {
                return Flux.error(new IllegalStateException("can not send messages because the connection is closed"));
            }

            return this.responseProcessor.doOnSubscribe(s -> send(requests).subscribe());
        });
    }

    @Override
    public Mono<Void> sendOnly(Publisher<? extends SendOnlyMessage> messages) {
        requireNonNull(messages, "messages must not be null");

        return Mono.defer(() -> {
            if (this.closing.get()) {
                return Mono.error(new IllegalStateException("can not send messages because the connection is closed"));
            }

            return send(messages);
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
            return send(Mono.just(ExitMessage.getInstance())).as(it -> {
                if (logger.isDebugEnabled()) {
                    return it.doOnSuccess(ignored -> logger.debug("Exit message has been sent successfully, close the connection"));
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
    public void sslUnsupported() {
        connection.channel().pipeline().fireUserEventTriggered(SslState.UNSUPPORTED);
    }

    @Override
    public void loginSuccess() {
        connection.channel().pipeline().fireUserEventTriggered(Lifecycle.COMMAND);
    }

    @Override
    public String toString() {
        return String.format("ReactorNettyClient(%s){connectionId=%d}", this.closing.get() ? "closing or closed" : "activating", session.getConnectionId());
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
}
