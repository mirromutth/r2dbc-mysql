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

import io.github.mirromutth.r2dbc.mysql.config.ConnectProperties;
import io.github.mirromutth.r2dbc.mysql.constant.ProtocolVersion;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.github.mirromutth.r2dbc.mysql.session.ServerVersion;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;

import static java.util.Objects.requireNonNull;

/**
 * An abstraction that wraps the networking part of exchanging methods.
 */
public interface Client {

    Flux<BackendMessage> exchange(Publisher<FrontendMessage> requests);

    Mono<Void> close();

    Mono<ProtocolVersion> getProtocolVersion();

    Mono<Integer> getConnectionId();

    Mono<ServerVersion> getServerVersion();

    static Mono<Client> connect(String host, int port, ConnectProperties properties) {
        return connect(ConnectionProvider.newConnection(), host, port, properties);
    }

    static Mono<Client> connect(ConnectionProvider connectionProvider, String host, int port, ConnectProperties properties) {
        requireNonNull(connectionProvider, "connectionProvider must not be null");
        requireNonNull(host, "host must not be null");
        requireNonNull(properties, "properties must not be null");

        return TcpClient.create(connectionProvider)
            .host(host)
            .port(port)
            .connect()
            .map((connection) -> new ReactorNettyClient(connection, properties));
    }
}
