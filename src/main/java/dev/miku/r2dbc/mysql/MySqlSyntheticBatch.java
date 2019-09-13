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
import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.internal.ConnectionContext;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

import static dev.miku.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlBatch} for executing a collection of statements
 * in one-by-one against the MySQL database.
 */
final class MySqlSyntheticBatch extends MySqlBatch {

    private final Client client;

    private final Codecs codecs;

    private final ConnectionContext context;

    private final List<String> statements = new ArrayList<>();

    MySqlSyntheticBatch(Client client, Codecs codecs, ConnectionContext context) {
        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.context = requireNonNull(context, "context must not be null");
    }

    @Override
    public MySqlBatch add(String sql) {
        statements.add(sql);
        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        return SimpleQueryFlow.execute(client, statements)
            .windowUntil(SimpleQueryFlow.RESULT_DONE)
            .map(messages -> new MySqlResult(false, codecs, context, null, messages));
    }

    @Override
    public String toString() {
        return "MySqlSyntheticBatch{sql=REDACTED}";
    }
}
