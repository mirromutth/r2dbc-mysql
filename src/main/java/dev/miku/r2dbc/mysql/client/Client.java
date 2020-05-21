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

import dev.miku.r2dbc.mysql.MySqlSslConfiguration;
import dev.miku.r2dbc.mysql.message.client.ExchangeableMessage;
import dev.miku.r2dbc.mysql.message.client.SendOnlyMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.ConnectionContext;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;
import reactor.util.annotation.Nullable;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An abstraction that wraps the networking part of exchanging methods.
 */
public interface Client {

    /**
     * Perform an exchange of message. Calling this method while a previous exchange is active will
     * return a deferred handle and queue the request until the previous exchange terminates.
     *
     * @param request  one request for get server responses
     * @param complete determining the last response frame to {@code Subscriber#onComplete()}
     *                 complete the stream and prevent multiple subscribers from consuming
     *                 previous, active response streams
     * @return A {@link Flux} of incoming messages that ends with the end of the frame
     */
    Flux<ServerMessage> exchange(ExchangeableMessage request, Predicate<ServerMessage> complete);

    Mono<Void> sendOnly(SendOnlyMessage message);

    Mono<ServerMessage> receiveOnly();

    Mono<Void> close();

    Mono<Void> forceClose();

    /**
     * Returns the {@link ByteBufAllocator}.
     *
     * @return the {@link ByteBufAllocator}
     */
    ByteBufAllocator getByteBufAllocator();

    boolean isConnected();

    void sslUnsupported();

    void loginSuccess();

    static Mono<Client> connect(SocketAddress address, MySqlSslConfiguration ssl, ConnectionContext context, @Nullable Duration connectTimeout) {
        requireNonNull(address, "address must not be null");
        requireNonNull(ssl, "ssl must not be null");
        requireNonNull(context, "context must not be null");

        return TcpClient.newConnection()
            .bootstrap(b -> {
                if (connectTimeout != null) {
                    b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.toMillis()));
                }

                return b.remoteAddress(address);
            })
            .connect()
            .map(conn -> new ReactorNettyClient(conn, ssl, context));
    }
}
