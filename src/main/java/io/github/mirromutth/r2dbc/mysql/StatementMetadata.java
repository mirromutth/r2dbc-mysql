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
import io.github.mirromutth.r2dbc.mysql.message.client.PreparedCloseMessage;
import reactor.core.publisher.Mono;

/**
 * Metadata for prepared statement considers close logic and {@code sql} holding.
 */
final class StatementMetadata {

    private final Client client;

    private final String sql;

    private final int statementId;

    StatementMetadata(Client client, String sql, int statementId) {
        this.client = client;
        this.sql = sql;
        this.statementId = statementId;
    }

    String getSql() {
        return sql;
    }

    int getStatementId() {
        return statementId;
    }

    Mono<Void> close() {
        // Note: close statement is idempotent.
        return client.sendOnly(new PreparedCloseMessage(statementId));
    }
}
