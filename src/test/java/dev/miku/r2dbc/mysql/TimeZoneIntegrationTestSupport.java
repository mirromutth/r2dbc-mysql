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

package dev.miku.r2dbc.mysql;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class considers integration tests for time zone conversion.
 */
abstract class TimeZoneIntegrationTestSupport extends IntegrationTestSupport {

    private static final ZoneId SERVER_ZONE_ID = ZoneId.of("GMT+6");

    static {
        // This timezone is just a time zone for tests.
        System.setProperty("user.timezone", "GMT+2");
    }

    TimeZoneIntegrationTestSupport(@Nullable Predicate<String> preferPrepared) {
        // This timezone is just a time zone for tests.
        super(configuration(false, SERVER_ZONE_ID, preferPrepared));
    }

    @Test
    void queryInstant() {
        LocalDateTime serverLocal = LocalDateTime.of(2020, 6, 17, 15, 0, 0);
        String serverTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(serverLocal);
        complete(connection -> Flux.from(connection.createStatement("CREATE TEMPORARY TABLE test (id INT PRIMARY KEY AUTO_INCREMENT, value TIMESTAMP)")
            .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement(String.format("INSERT INTO test VALUE (DEFAULT, '%s')", serverTime))
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id = ?")
                .bind(0, 1)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, Instant.class)))
            .map(it -> LocalDateTime.ofInstant(it, SERVER_ZONE_ID))
            .doOnNext(it -> assertThat(it).isEqualTo(serverLocal)));
    }

    @Test
    void updateInstant() {
        LocalDateTime clientLocal = LocalDateTime.of(2020, 6, 17, 15, 0, 0);
        complete(connection -> Flux.from(connection.createStatement("CREATE TEMPORARY TABLE test (id INT PRIMARY KEY AUTO_INCREMENT, value TIMESTAMP)")
            .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("INSERT INTO test VALUE (DEFAULT, ?)")
                .bind(0, toInstant(clientLocal))
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id = ?")
                .bind(0, 1)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, Instant.class)))
            .map(it -> LocalDateTime.ofInstant(it, ZoneId.systemDefault()))
            .doOnNext(it -> assertThat(it).isEqualTo(clientLocal)));
    }

    private static Instant toInstant(LocalDateTime value) {
        return value.toInstant(ZoneId.systemDefault().getRules().getOffset(value));
    }
}
