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

import reactor.core.publisher.Flux;

import reactor.util.annotation.Nullable;

/**
 * An implementation of {@link MySqlStatement} representing the simple query that has no parameter.
 */
final class SimpleQueryMySqlStatement implements MySqlStatement {

    @Override
    public SimpleQueryMySqlStatement add() {
        // TODO: implement this method
        return this;
    }

    @Override
    public SimpleQueryMySqlStatement bind(@Nullable Object identifier, @Nullable Object value) {
        // TODO: implement this method
        return this;
    }

    @Override
    public SimpleQueryMySqlStatement bind(int index, @Nullable Object value) {
        // TODO: implement this method
        return this;
    }

    @Override
    public SimpleQueryMySqlStatement bindNull(@Nullable Object identifier, @Nullable Class<?> type) {
        // TODO: implement this method
        return this;
    }

    @Override
    public SimpleQueryMySqlStatement bindNull(int index, @Nullable Class<?> type) {
        // TODO: implement this method
        return this;
    }

    @Override
    public SimpleQueryMySqlStatement returnGeneratedValues(String... columns) {
        // TODO: implement this method
        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        // TODO: implement this method
        return Flux.empty();
    }
}
