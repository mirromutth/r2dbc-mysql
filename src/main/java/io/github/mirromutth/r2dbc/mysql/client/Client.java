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

import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.client.ExchangeableMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.netty.channel.ChannelOption;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.annotation.Nullable;

import java.time.Duration;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An abstraction that wraps the networking part of exchanging methods.
 */
public interface Client {

    /**
     * @param requests single or multi request(s) for get the server responses.
     * @return The result should be truncated, otherwise it will NEVER be completed.
     */
    Flux<ServerMessage> exchange(Publisher<? extends ExchangeableMessage> requests);

    Mono<Void> initialize();

    Mono<Void> close();

    static Mono<Client> connect(
        ConnectionProvider connectionProvider,
        String host,
        int port,
        @Nullable Duration connectTimeout,
        MySqlSession session
    ) {
        requireNonNull(connectionProvider, "connectionProvider must not be null");
        requireNonNull(host, "host must not be null");
        TcpClient client = TcpClient.create(connectionProvider);

        if (connectTimeout != null) {
            client = client.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.toMillis()));
        }

        return client.host(host)
            .port(port)
            .connect()
            .map(conn -> new ReactorNettyClient(conn, session));
    }
}
