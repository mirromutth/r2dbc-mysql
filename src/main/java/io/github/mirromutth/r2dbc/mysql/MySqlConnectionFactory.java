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
import io.github.mirromutth.r2dbc.mysql.config.ConnectProperties;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.json.MySqlJsonFactory;
import io.github.mirromutth.r2dbc.mysql.message.header.SequenceIdProvider;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a MySQL database.
 * <p>
 * Note: Can NOT be {@code final} because this class maybe used by IoC/AoP frameworks.
 */
public class MySqlConnectionFactory implements ConnectionFactory {

    private final ConnectionProvider connectionProvider;

    private final MySqlJsonFactory mySqlJsonFactory;

    private final ConnectProperties connectProperties;

    public MySqlConnectionFactory(ConnectionProvider connectionProvider, MySqlJsonFactory mySqlJsonFactory, ConnectProperties connectProperties) {
        this.connectionProvider = requireNonNull(connectionProvider, "connectionProvider must not be null");
        this.mySqlJsonFactory = requireNonNull(mySqlJsonFactory, "mySqlJsonFactory must not be null");
        this.connectProperties = requireNonNull(connectProperties, "connectProperties must not be null");
    }

    @Override
    public Mono<MySqlConnection> create() {
        return Mono.defer(() -> {
            MySqlSession session = new MySqlSession(
                connectProperties.isUseSsl(),
                connectProperties.getDatabase(),
                connectProperties.getZeroDateOption(),
                connectProperties.getUsername(),
                connectProperties.getPassword()
            );
            String host = connectProperties.getHost();
            int port = connectProperties.getPort();
            Duration connectTimeout = connectProperties.getTcpConnectTimeout();

            return Client.connect(connectionProvider, host, port, connectTimeout, session)
                .flatMap(client -> client.initialize().thenReturn(client))
                .map(client -> new MySqlConnection(client, session, mySqlJsonFactory));
        });
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return MySqlConnectionFactoryMetadata.INSTANCE;
    }
}
