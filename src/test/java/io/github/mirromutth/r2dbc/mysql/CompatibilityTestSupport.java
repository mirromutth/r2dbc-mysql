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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Mono;

/**
 * Base class considers connection factory and general function for compatibility unit tests.
 */
abstract class CompatibilityTestSupport {

    final MySqlConnectionFactory connectionFactory;

    CompatibilityTestSupport(MySqlConnectionConfiguration configuration) {
        this.connectionFactory = MySqlConnectionFactory.from(configuration);
    }

    static <T> Mono<T> close(Connection connection) {
        return Mono.from(connection.close()).then(Mono.empty());
    }

    static Mono<Integer> extractRowsUpdated(Result result) {
        return Mono.from(result.getRowsUpdated());
    }
}
