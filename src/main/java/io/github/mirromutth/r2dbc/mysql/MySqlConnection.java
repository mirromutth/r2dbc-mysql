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
import io.github.mirromutth.r2dbc.mysql.constant.Capability;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.SslRequestMessage;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import reactor.core.publisher.Mono;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Connection} for connecting to the MySQL database.
 */
public final class MySqlConnection implements Connection {

    MySqlConnection(Client client, ConnectProperties properties) {
        requireNonNull(client, "client must not be null");
        requireNonNull(properties, "properties must not be null");

        // TODO: HandshakeResponseMessage
        client.getSession().flatMap(session -> {
            int clientCapabilities = calculateCapabilities(session.getServerCapabilities(), properties);

            if ((clientCapabilities & Capability.SSL.getFlag()) != 0) {
                Mono<FrontendMessage> ssl = Mono.just(new SslRequestMessage(clientCapabilities, (byte) session.getCollation().getId()));
                return client.exchange(ssl).then(Mono.just(clientCapabilities));
            } else {
                return Mono.just(clientCapabilities);
            }
        }).subscribe();
    }

    @Override
    public Mono<Void> beginTransaction() {
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public Mono<Void> close() {
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public Mono<Void> commitTransaction() {
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public MySqlBatch createBatch() {
        // TODO: implement this method
        return new MySqlBatch();
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        requireNonNull(name, "name must not be null");
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public MySqlStatement createStatement(String sql) {
        requireNonNull(sql, "sql must not be null");
        // TODO: implement this method
        return new SimpleQueryMySqlStatement();
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        requireNonNull(name, "name must not be null");
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        requireNonNull(name, "name must not be null");
        // TODO: implement this method
        return Mono.empty();
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        requireNonNull(isolationLevel, "isolationLevel must not be null");
        // TODO: implement this method
        return Mono.empty();
    }

    private static int calculateCapabilities(int serverCapabilities, ConnectProperties properties) {
        int clientCapabilities = serverCapabilities;

        if (!properties.isUseSsl()) {
            clientCapabilities &= ~Capability.SSL.getFlag();
        }

        if (properties.getDatabase().isEmpty()) {
            clientCapabilities &= ~Capability.CONNECT_WITH_DB.getFlag();
        }

        if (properties.getAttributes().isEmpty()) {
            clientCapabilities &= ~Capability.CONNECT_ATTRS.getFlag();
        }

        return clientCapabilities;
    }
}
