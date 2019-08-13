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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A implementation of {@link QueryCache} for cache that parsed SQL queries eternally.
 */
final class IndefiniteQueryCache extends ConcurrentHashMap<String, Query> implements QueryCache {

    private static final Function<String, Query> MAPPING = Query::parse;

    @Override
    public Query getOrParse(String sql) {
        return computeIfAbsent(sql, MAPPING);
    }
}
