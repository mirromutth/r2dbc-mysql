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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.github.mirromutth.r2dbc.mysql.MySqlConnectionRunner.STD5_7;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for ping message, include application level ping {@code SELECT 1}.
 * <p>
 * SQL query {@code SELECT 1} is a ping approach which is cross-database (maybe some are not
 * supported) and is easy to understand.
 * <p>
 * Note: looks like {@code SELECT 1} result value type returned by the MySQL server is BIGINT,
 * try using Number.class to eliminate {@code assertEquals} fail because of the value type.
 *
 * @see MySqlConnection#ping() native command "ping"
 */
class PingPongTest {

    @Test
    void selectOne() throws Throwable {
        STD5_7.run(Duration.ofSeconds(4), connection -> Mono.from(connection.createStatement("SELECT 1").execute())
            .flatMapMany(result -> result.map((row, metadata) -> row.get(0, Number.class)))
            .doOnNext(number -> assertEquals(number.intValue(), 1))
            .reduce((x, y) -> Math.addExact(x.intValue(), y.intValue()))
            .doOnNext(number -> assertEquals(number.intValue(), 1)));
    }

    @Test
    void realPing() throws Throwable {
        STD5_7.run(Duration.ofSeconds(4), MySqlConnection::ping);
    }
}
