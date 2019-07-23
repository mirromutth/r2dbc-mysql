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
import io.github.mirromutth.r2dbc.mysql.codec.Codecs;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Batch} for executing a collection of statements in a batch against the MySQL database.
 */
public final class MySqlBatch implements Batch {

    private final Client client;

    private final Codecs codecs;

    private final MySqlSession session;

    private final List<String> statements = new ArrayList<>();

    MySqlBatch(Client client, Codecs codecs, MySqlSession session) {
        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.session = requireNonNull(session, "session must not be null");
    }

    /**
     * @param sql should contain only one-statement.
     * @return this {@link MySqlBatch}
     * @throws IllegalArgumentException if {@code sql} is {@code null} or contain multi-statements.
     */
    @Override
    public MySqlBatch add(String sql) {
        statements.add(Queries.formatBatchElement(sql));
        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        return SimpleQueryFlow.execute(client, statements, session)
            .map(messages -> new MySqlResult(codecs, session, null, messages));
    }

    @Override
    public String toString() {
        return String.format("MySqlBatch{ has %d statements }", statements.size());
    }
}
