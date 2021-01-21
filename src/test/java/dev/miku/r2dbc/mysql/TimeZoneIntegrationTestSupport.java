/*
 * Copyright 2018-2021 the original author or authors.
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
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.TimeZone;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class considers integration tests for time zone conversion.
 */
abstract class TimeZoneIntegrationTestSupport extends IntegrationTestSupport {

    private static final String TIMESTAMP_TABLE = "CREATE TEMPORARY TABLE test " +
        "(id INT PRIMARY KEY AUTO_INCREMENT, value TIMESTAMP)";

    private static final String TIME_TABLE = "CREATE TEMPORARY TABLE test " +
        "(id INT PRIMARY KEY AUTO_INCREMENT, value TIME)";

    private static final LocalDateTime ST = LocalDateTime.of(2019, 1, 1, 11, 59, 59);

    private static final LocalDateTime DST = ST.plusHours(200 * 24);

    private static final String INSERT = String.format(
        "INSERT INTO test VALUE (DEFAULT, '%s'), (DEFAULT, '%s')",
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(ST),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(DST));

    private static final ZoneId SERVER_ZONE = ZoneId.of("America/New_York");

    static {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+6"));

        // Make sure test cases contains daylight.
        assertThat(ST.atZone(SERVER_ZONE).plusHours(200 * 24))
            .isEqualTo(DST.atZone(SERVER_ZONE).plusHours(1));
    }

    TimeZoneIntegrationTestSupport(@Nullable Predicate<String> preferPrepared) {
        super(configuration(false, SERVER_ZONE, preferPrepared));
    }

    @Test
    void queryZonedDateTime() {
        complete(connection -> Flux.from(connection.createStatement(TIMESTAMP_TABLE).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement(INSERT).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, ZonedDateTime.class)))
            .doOnNext(it -> assertThat(it.getZone()).isEqualTo(SERVER_ZONE))
            .map(ZonedDateTime::toLocalDateTime)
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Arrays.asList(ST, DST))));
    }

    @Test
    void queryOffsetDateTime() {
        complete(connection -> Flux.from(connection.createStatement(TIMESTAMP_TABLE).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement(INSERT).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, OffsetDateTime.class)))
            .doOnNext(it -> assertThat(it.getOffset())
                .isEqualTo(SERVER_ZONE.getRules().getOffset(it.toLocalDateTime())))
            .map(OffsetDateTime::toLocalDateTime)
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Arrays.asList(ST, DST))));
    }

    @Test
    void queryInstant() {
        complete(connection -> Flux.from(connection.createStatement(TIMESTAMP_TABLE).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement(INSERT).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, Instant.class)))
            .map(it -> LocalDateTime.ofInstant(it, SERVER_ZONE))
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Arrays.asList(ST, DST))));
    }

    @Test
    void queryOffsetTime() {
        complete(connection -> Flux.from(connection.createStatement(TIME_TABLE).execute())
            .thenMany(connection.createStatement("INSERT INTO test VALUE (DEFAULT, '11:23:58')")
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, OffsetTime.class)))
            .doOnNext(it -> assertThat(it.getOffset())
                .isEqualTo(SERVER_ZONE.getRules().getStandardOffset(Instant.EPOCH)))
            .map(OffsetTime::toLocalTime)
            .collectList()
            .doOnNext(it -> assertThat(it)
                .isEqualTo(Collections.singletonList(LocalTime.of(11, 23, 58)))));
    }

    @Test
    void updateZonedDateTime() {
        complete(connection -> Flux.from(connection.createStatement(TIMESTAMP_TABLE).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("INSERT INTO test VALUE (DEFAULT, ?)")
                .bind(0, toZonedDateTime(ST))
                .add()
                .bind(0, toZonedDateTime(DST))
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, ZonedDateTime.class)))
            .doOnNext(it -> assertThat(it.getZone()).isEqualTo(SERVER_ZONE))
            .map(it -> it.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime())
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Arrays.asList(ST, DST))));
    }

    @Test
    void updateOffsetDateTime() {
        complete(connection -> Flux.from(connection.createStatement(TIMESTAMP_TABLE).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("INSERT INTO test VALUE (DEFAULT, ?)")
                .bind(0, toOffsetDateTime(ST))
                .add()
                .bind(0, toOffsetDateTime(DST))
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, OffsetDateTime.class)))
            .doOnNext(it -> assertThat(it.getOffset())
                .isEqualTo(SERVER_ZONE.getRules().getOffset(it.toLocalDateTime())))
            .map(it -> it.toZonedDateTime().withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime())
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Arrays.asList(ST, DST))));
    }

    @Test
    void updateInstant() {
        complete(connection -> Flux.from(connection.createStatement(TIMESTAMP_TABLE).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("INSERT INTO test VALUE (DEFAULT, ?)")
                .bind(0, toInstant(ST))
                .add()
                .bind(0, toInstant(DST))
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, Instant.class)))
            .map(it -> LocalDateTime.ofInstant(it, ZoneId.systemDefault()))
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Arrays.asList(ST, DST))));
    }

    @Test
    void updateOffsetTime() {
        complete(connection -> Flux.from(connection.createStatement(TIME_TABLE).execute())
            .thenMany(connection.createStatement("INSERT INTO test VALUE (DEFAULT, ?)")
                .bind(0, OffsetTime.of(11, 23, 58, 0,
                    ZoneId.systemDefault().getRules().getStandardOffset(Instant.EPOCH)))
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, OffsetTime.class)))
            .doOnNext(it -> assertThat(it.getOffset())
                .isEqualTo(SERVER_ZONE.getRules().getStandardOffset(Instant.EPOCH)))
            .map(it -> it.withOffsetSameInstant(ZoneId.systemDefault().getRules()
                .getStandardOffset(Instant.EPOCH))
                .toLocalTime())
            .collectList()
            .doOnNext(it -> assertThat(it)
                .isEqualTo(Collections.singletonList(LocalTime.of(11, 23, 58)))));
    }

    private static Instant toInstant(LocalDateTime value) {
        return value.toInstant(ZoneId.systemDefault().getRules().getOffset(value));
    }

    private static ZonedDateTime toZonedDateTime(LocalDateTime value) {
        return value.atZone(ZoneId.systemDefault());
    }

    private static OffsetDateTime toOffsetDateTime(LocalDateTime value) {
        return value.atOffset(ZoneId.systemDefault().getRules().getOffset(value));
    }
}
