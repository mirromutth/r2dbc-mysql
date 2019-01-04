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

import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

/**
 * An implementation of {@link Batch} for executing a collection of statements in a batch against the MySQL database.
 */
public final class MySqlBatch implements Batch<MySqlBatch> {

    @Override
    public MySqlBatch add(String s) {
        // TODO: implement this method
        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        // TODO: implement this method
        return Flux.empty();
    }
}
