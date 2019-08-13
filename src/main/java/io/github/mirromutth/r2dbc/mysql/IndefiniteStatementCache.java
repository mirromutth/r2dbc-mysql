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
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.function.Function;

/**
 * A implementation of {@link StatementCache} for cache that stores prepared statement handles eternally.
 */
final class IndefiniteStatementCache extends HashMap<String, Mono<Integer>> implements StatementCache {

    private final Function<String, Mono<Integer>> mapping;

    IndefiniteStatementCache(Client client) {
        this.mapping = sql -> QueryFlow.prepare(client, sql).cache();
    }

    @Override
    public Mono<Integer> getOrPrepare(String sql) {
        return computeIfAbsent(sql, mapping);
    }
}
