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

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Result} representing the results of a query against the MySQL database.
 */
public final class MySqlResult implements Result {

    private final MySqlRowMetadata rowMetadata;

    private final Flux<MySqlRow> rows;

    public MySqlResult(MySqlRowMetadata rowMetadata, Flux<MySqlRow> rows) {
        this.rowMetadata = requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.rows = requireNonNull(rows, "rows must not be null");
    }

    @Override
    public Mono<Integer> getRowsUpdated() {
        // TODO: implement this method
        return Mono.empty();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        requireNonNull(f, "f must not be null");
        return rows.map(row -> f.apply(row, rowMetadata));
    }
}
