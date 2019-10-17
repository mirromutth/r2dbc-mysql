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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

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
            MySqlSslConfiguration ssl;
            SocketAddress address;

            if (configuration.isHost()) {
                ssl = configuration.getSsl();
                address = InetSocketAddress.createUnresolved(configuration.getDomain(), configuration.getPort());
            } else {
                ssl = MySqlSslConfiguration.disabled();
                address = new DomainSocketAddress(configuration.getDomain());
            }

            String database = configuration.getDatabase();
            String username = configuration.getUsername();
            CharSequence password = configuration.getPassword();
            SslMode sslMode = ssl.getSslMode();
            ConnectionContext context = new ConnectionContext(configuration.getZeroDateOption());

            return Client.connect(address, ssl, context, configuration.getConnectTimeout())
                .flatMap(client -> LoginFlow.login(client, sslMode, database, context, username, password))
                .flatMap(client -> MySqlConnection.create(client, context));
        }));
    }
}
