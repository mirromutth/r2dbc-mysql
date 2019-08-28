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

import io.github.mirromutth.r2dbc.mysql.constant.CacheType;

/**
 * A cache interface considers that cache parsed {@link Query} for multi-connections
 * which from the same {@link MySqlConnectionFactory}.
 */
interface QueryCache {

    /**
     * @param sql origin SQL
     * @return {@code null} means {@code sql} is not a prepare query
     */
    Query getOrParse(String sql);

    static QueryCache create(CacheConfiguration configuration) {
        CacheType type = configuration.getType();

        switch (type) {
            case INDEFINITE:
                return new IndefiniteQueryCache();
            case W_TINY_LFU:
                return new WindowTinyLfuQueryCache(configuration.getSize());
            default:
                throw new IllegalStateException("Unsupported cache type " + type);
        }
    }
}
