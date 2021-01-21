/*
 * Copyright 2018-2021 the original author or authors.
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
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.tcp.TcpClient;
import reactor.util.annotation.Nullable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.function.BiConsumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An abstraction that wraps the networking part of exchanging methods.
 */
public interface Client {

    /**
     * Perform an exchange of a request message. Calling this method while a previous exchange is active will
     * return a deferred handle and queue the request until the previous exchange terminates.
     *
     * @param request one and only one request message for get server responses
     * @param handler response handler, {@link SynchronousSink#complete()} should be called after the last
     *                response frame is sent to complete the stream and prevent multiple subscribers from
     *                consuming previous, active response streams
     * @param <T>     handling response type
     * @return A {@link Flux} of incoming messages that ends with the end of the frame
     */
    <T> Flux<T> exchange(ClientMessage request, BiConsumer<ServerMessage, SynchronousSink<T>> handler);

    /**
     * Perform an exchange of multi-request messages. Calling this method while a previous exchange is active
     * will return a deferred handle and queue the request until the previous exchange terminates.
     *
     * @param exchangeable request messages and response handler
     * @param <T>          handling response type
     * @return A {@link Flux} of incoming messages that ends with the end of the frame
     */
    <T> Flux<T> exchange(FluxExchangeable<T> exchangeable);

    /**
     * Close the connection of the {@link Client} with close request.
     * <p>
     * Notice: should not use it before connection login phase.
     *
     * @return A {@link Mono} that will emit a complete signal after connection closed
     */
    Mono<Void> close();

    /**
     * Force close the connection of the {@link Client}. It is useful when login phase emit an error.
     *
     * @return A {@link Mono} that will emit a complete signal after connection closed
     */
    Mono<Void> forceClose();

    /**
     * Returns the {@link ByteBufAllocator}.
     *
     * @return the {@link ByteBufAllocator}
     */
    ByteBufAllocator getByteBufAllocator();

    /**
     * Local check connection is valid or not.
     *
     * @return if connection is valid
     */
    boolean isConnected();

    /**
     * Send a signal to {@code this}, which means server does not support SSL.
     */
    void sslUnsupported();

    /**
     * Send a signal to {@code this}, which means login has succeed.
     */
    void loginSuccess();

    /**
     * Connect to {@code address} with configurations. Normally, should login after connected.
     *
     * @param ssl            the SSL configuration
     * @param address        socket address, may be host address, or Unix Domain Socket address
     * @param tcpKeepAlive   if enable the {@link ChannelOption#SO_KEEPALIVE}
     * @param tcpNoDelay     if enable the {@link ChannelOption#TCP_NODELAY}
     * @param context        the connection context
     * @param connectTimeout connect timeout, or {@code null} if has no timeout
     * @return A {@link Mono} that will emit a connected {@link Client}.
     * @throws IllegalArgumentException if {@code ssl}, {@code address} or {@code context} is {@code null}.
     * @throws ArithmeticException      if {@code connectTimeout} milliseconds overflow as an int
     */
    static Mono<Client> connect(MySqlSslConfiguration ssl, SocketAddress address, boolean tcpKeepAlive,
        boolean tcpNoDelay, ConnectionContext context, @Nullable Duration connectTimeout) {
        requireNonNull(ssl, "ssl must not be null");
        requireNonNull(address, "address must not be null");
        requireNonNull(context, "context must not be null");

        TcpClient tcpClient = TcpClient.newConnection();

        if (connectTimeout != null) {
            tcpClient = tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                Math.toIntExact(connectTimeout.toMillis()));
        }

        if (address instanceof InetSocketAddress) {
            tcpClient = tcpClient.option(ChannelOption.SO_KEEPALIVE, tcpKeepAlive);
            tcpClient = tcpClient.option(ChannelOption.TCP_NODELAY, tcpNoDelay);
        }

        return tcpClient.remoteAddress(() -> address).connect()
            .map(conn -> new ReactorNettyClient(conn, ssl, context));
    }
}
