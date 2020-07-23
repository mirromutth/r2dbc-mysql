/*
 * Copyright 2018-2020 the original author or authors.
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

package dev.miku.r2dbc.mysql.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * An unbounded implementation of {@link QueryCache}.
 */
final class QueryUnboundedCache<T> extends ConcurrentHashMap<String, T> implements QueryCache<T> {

    private final Function<String, T> mapping;

    QueryUnboundedCache(Function<String, T> mapping) {
        this.mapping = mapping;
    }

    @Override
    public T get(String key) {
        // An optimistic fast path to avoid unnecessary locking.
        T value = super.get(key);
        return value == null ? super.computeIfAbsent(key, mapping) : value;
    }
}
