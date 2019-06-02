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

package io.github.mirromutth.r2dbc.mysql;

import io.github.mirromutth.r2dbc.mysql.client.Client;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a MySQL database.
 */
public final class MySqlConnectionFactory implements ConnectionFactory {

    private final MySqlConnectConfiguration configuration;

    public MySqlConnectionFactory(MySqlConnectConfiguration configuration) {
        this.configuration = requireNonNull(configuration, "configuration must not be null");
    }

    @Override
    public Mono<MySqlConnection> create() {
        return Mono.defer(() -> {
            MySqlSession session = new MySqlSession(
                configuration.isUseSsl(),
                configuration.getDatabase(),
                configuration.getZeroDate(),
                configuration.getUsername(),
                configuration.getPassword()
            );
            String host = configuration.getHost();
            int port = configuration.getPort();
            Duration connectTimeout = configuration.getConnectTimeout();

            return Client.connect(ConnectionProvider.newConnection(), host, port, connectTimeout, session)
                .flatMap(client -> client.initialize().thenReturn(client))
                .map(client -> new MySqlConnection(client, session));
        });
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return MySqlConnectionFactoryMetadata.INSTANCE;
    }
}
