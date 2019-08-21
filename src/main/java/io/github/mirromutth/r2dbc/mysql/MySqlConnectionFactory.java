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
import io.github.mirromutth.r2dbc.mysql.constant.SslMode;
import io.github.mirromutth.r2dbc.mysql.internal.ConnectionContext;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a MySQL database.
 */
public final class MySqlConnectionFactory implements ConnectionFactory {

    private final Mono<MySqlConnection> client;

    private MySqlConnectionFactory(Mono<MySqlConnection> client) {
        this.client = client;
    }

    @Override
    public Mono<MySqlConnection> create() {
        return client;
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return MySqlConnectionFactoryMetadata.INSTANCE;
    }

    public static MySqlConnectionFactory from(MySqlConnectionConfiguration configuration) {
        requireNonNull(configuration, "configuration must not be null");

        return new MySqlConnectionFactory(Mono.defer(() -> {
            ConnectionContext context = new ConnectionContext(configuration.getDatabase(), configuration.getZeroDateOption());
            String host = configuration.getHost();
            int port = configuration.getPort();
            Duration connectTimeout = configuration.getConnectTimeout();
            MySqlSslConfiguration ssl = configuration.getSsl();
            SslMode sslMode = ssl.getSslMode();
            String username = configuration.getUsername();
            CharSequence password = configuration.getPassword();

            return Client.connect(ConnectionProvider.newConnection(), host, port, ssl, context, connectTimeout)
                .flatMap(client -> LoginFlow.login(client, sslMode, context, username, password))
                .flatMap(client -> MySqlConnection.create(client, context));
        }));
    }
}
