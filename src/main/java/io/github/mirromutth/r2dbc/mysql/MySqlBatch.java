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
import io.github.mirromutth.r2dbc.mysql.converter.Converters;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

import java.util.StringJoiner;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Batch} for executing a collection of statements in a batch against the MySQL database.
 */
public final class MySqlBatch implements Batch {

    private final Client client;

    private final Converters converters;

    private final MySqlSession session;

    private int count = 0;

    private final StringJoiner statements = new StringJoiner(";");

    MySqlBatch(Client client, Converters converters, MySqlSession session) {
        this.client = requireNonNull(client, "client must not be null");
        this.converters = requireNonNull(converters, "converters must not be null");
        this.session = requireNonNull(session, "session must not be null");
    }

    /**
     * @param sql should include only one-statement, otherwise stream terminate disordered.
     * @return this {@link MySqlBatch}
     * @throws IllegalArgumentException if {@code sql} is {@code null}
     */
    @Override
    public MySqlBatch add(String sql) {
        requireNonNull(sql, "SQL must not be null");

        ++this.count;
        this.statements.add(sql);
        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        return Flux.defer(() -> {
            int count = this.count;

            if (count <= 0) {
                return Flux.empty();
            }

            return SimpleQueryFlow.execute(this.client, this.statements.toString())
                .windowUntil(message -> message instanceof OkMessage)
                .take(count)
                .map(messages -> new SimpleMySqlResult(this.converters, this.session, messages));
        });
    }

    @Override
    public String toString() {
        return "MySqlBatch{" +
            "client=" + client +
            ", statements=<hidden>" +
            '}';
    }
}
