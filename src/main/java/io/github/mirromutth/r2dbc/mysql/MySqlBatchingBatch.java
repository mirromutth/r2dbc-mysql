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
import io.github.mirromutth.r2dbc.mysql.internal.ConnectionContext;
import reactor.core.publisher.Flux;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlBatch} for executing a collection of statements
 * in a batch against the MySQL database.
 */
final class MySqlBatchingBatch extends MySqlBatch {

    private final Client client;

    private final Codecs codecs;

    private final ConnectionContext context;

    private StringBuilder builder;

    MySqlBatchingBatch(Client client, Codecs codecs, ConnectionContext context) {
        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.context = requireNonNull(context, "context must not be null");
    }

    @Override
    public MySqlBatch add(String sql) {
        requireNonNull(sql, "sql must not be null");

        int index = lastNonWhitespace(sql);

        if (index >= 0 && sql.charAt(index) == ';') {
            // Skip last ';' and whitespaces that following last ';'.
            requireBuilder().append(sql, 0, index);
        } else {
            requireBuilder().append(sql);
        }

        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        return SimpleQueryFlow.execute(client, getSql())
            .windowUntil(SimpleQueryFlow.RESULT_DONE)
            .map(messages -> new MySqlResult(false, codecs, context, null, messages));
    }

    @Override
    public String toString() {
        return "MySqlBatchingBatch{sql=REDACTED}";
    }

    /**
     * Accessible for unit test.
     *
     * @return current batching SQL statement
     */
    String getSql() {
        return builder.toString();
    }

    private StringBuilder requireBuilder() {
        if (builder == null) {
            return (builder = new StringBuilder());
        }

        return builder.append(';');
    }

    private static int lastNonWhitespace(String sql) {
        int size = sql.length();

        for (int i = size - 1; i >= 0; --i) {
            if (!Character.isWhitespace(sql.charAt(i))) {
                return i;
            }
        }

        return -1;
    }
}
