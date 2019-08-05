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
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.test.Example;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Extra unit tests for date/time which append to TCK.
 */
interface MySqlExampleDateTimeExtra extends MySqlExampleExtra {

    Logger logger = LoggerFactory.getLogger(MySqlExampleDateTimeExtra.class);

    DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .toFormatter();

    DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .appendLiteral(' ')
        .append(TIME_FORMATTER)
        .toFormatter();

    @Test
    default void dateTimeSimpleCrud() {
        int firstId = 11;

        getJdbcOperations().execute(String.format("DROP TABLE %s", "test"));
        getJdbcOperations().execute(String.format("CREATE TABLE test (id BIGINT AUTO_INCREMENT PRIMARY KEY,birth_year YEAR,birth_day DATE," +
            "birth_time TIME,birth_datetime DATETIME,birth_timestamp TIMESTAMP)AUTO_INCREMENT=%d", firstId));

        // Make sure that all date time is not equal.
        LocalDateTime firstBirth = LocalDateTime.of(1999, 12, 31, 23, 59, 59);
        LocalDateTime secondBirth = LocalDateTime.of(2012, 12, 21, 12, 12, 12);
        // Should avoid the 2038 problem.
        LocalDateTime thirdBirth = LocalDateTime.of(2028, 3, 4, 5, 6, 7);

        Mono.from(getConnectionFactory().create())
            .flatMap(connection -> {
                Statement insertFirstStmt = connection.createStatement(Entity.simpleInsert(firstBirth));
                Statement insertSecondStmt = connection.createStatement(Entity.simpleInsert(secondBirth));
                Statement selectStmt = connection.createStatement("SELECT * FROM `test` ORDER BY `id`");
                Statement updateStmt = connection.createStatement(Entity.simpleUpdate(thirdBirth));
                Statement deleteStmt = connection.createStatement("DELETE FROM `test` WHERE `id` > 0");

                return Mono.from(insertFirstStmt.execute())
                    .flatMap(Example::extractRowsUpdated)
                    .doOnNext(u -> assertEquals(u.intValue(), 1))
                    .then(Mono.from(insertSecondStmt.returnGeneratedValues("second_id").execute()))
                    .flatMap(result -> Example.extractRowsUpdated(result)
                        .doOnNext(u -> assertEquals(u.intValue(), 1))
                        .thenMany(result.map((row, metadata) -> row.get("second_id", Long.class)))
                        .doOnNext(id -> assertEquals(id.intValue(), firstId + 1))
                        .then())
                    .then(Mono.from(selectStmt.execute()))
                    .flatMapMany(Entity::from)
                    .collectList()
                    .doOnNext(entities -> assertEquals(entities.size(), 2))
                    .doOnNext(entities -> assertEquals(entities.get(0), new Entity(firstId, firstBirth)))
                    .doOnNext(entities -> assertEquals(entities.get(1), new Entity(firstId + 1, secondBirth)))
                    .then(Mono.from(updateStmt.execute()))
                    .flatMap(Example::extractRowsUpdated)
                    .doOnNext(u -> assertEquals(u.intValue(), 2))
                    .then(Mono.from(selectStmt.execute()))
                    .flatMapMany(Entity::from)
                    .collectList()
                    .doOnNext(entities -> assertEquals(entities.size(), 2))
                    .doOnNext(entities -> assertEquals(entities.get(0), new Entity(firstId, thirdBirth)))
                    .doOnNext(entities -> assertEquals(entities.get(1), new Entity(firstId + 1, thirdBirth)))
                    .then(Mono.from(deleteStmt.execute()))
                    .flatMap(Example::extractRowsUpdated)
                    .doOnNext(u -> assertEquals(u.intValue(), 2))
                    .concatWith(Example.close(connection))
                    .then();
            })
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    default void timePrepareCrud() {
        getJdbcOperations().execute(String.format("DROP TABLE %s", "test"));
        getJdbcOperations().execute(String.format("CREATE TABLE %s (id BIGINT AUTO_INCREMENT PRIMARY KEY,value TIME)", "test"));

        // -838:59:59.999999
        Duration value = Duration.ofSeconds(-TimeUnit.HOURS.toSeconds(838) - TimeUnit.MINUTES.toSeconds(59) - 60,
            (int) TimeUnit.MICROSECONDS.toNanos(1));
        Duration nanoRemoved = Entity.removeNano(value);
        LocalTime timeValue = Entity.toLocalTime(nanoRemoved);

        Mono.from(getConnectionFactory().create())
            .flatMap(connection -> Mono.from(connection.createStatement("INSERT INTO test VALUES (DEFAULT,?)")
                .bind(0, value)
                .execute())
                .flatMap(Example::extractRowsUpdated)
                .then(Mono.from(connection.createStatement("SELECT * FROM test WHERE id = ?")
                    .bind(0, 1)
                    .execute()))
                .flatMapMany(result -> result.map((r, m) -> Tuples.of(requireNonNull(r.get(1, Duration.class)), requireNonNull(r.get(1, LocalTime.class)))))
                .collectList()
                .map(values -> {
                    assertEquals(values.size(), 1);
                    return values.get(0);
                })
                .doOnNext(values -> {
                    if (value.equals(values.getT1())) {
                        logger.info("Server supported microseconds in TIME");
                        assertEquals(Entity.toLocalTime(value), values.getT2());
                    } else {
                        logger.warn("Server UNSUPPORTED microseconds in TIME");
                        assertEquals(nanoRemoved, values.getT1());
                        assertEquals(timeValue, values.getT2());
                    }
                })
                .then(Mono.from(connection.createStatement("SELECT * FROM test")
                    .execute()))
                .flatMapMany(result -> result.map((r, m) -> Tuples.of(requireNonNull(r.get(1, Duration.class)), requireNonNull(r.get(1, LocalTime.class)))))
                .collectList()
                .map(values -> {
                    assertEquals(values.size(), 1);
                    return values.get(0);
                })
                .doOnNext(values -> {
                    assertEquals(nanoRemoved, values.getT1());
                    assertEquals(timeValue, values.getT2());
                })
                .concatWith(Example.close(connection))
                .then())
            .as(StepVerifier::create)
            .verifyComplete();
    }

    final class Entity {

        private final long id;

        private final Year birthYear;

        private final LocalDate birthDay;

        /**
         * Same value of `birth_time`
         */
        private final Duration birthDuration;

        private final LocalTime birthTime;

        private final LocalDateTime birthDatetime;

        private final LocalDateTime birthTimestamp;

        private Entity(long id, LocalDateTime birth) {
            this(id, Year.of(birth.getYear()), birth.toLocalDate(), toDuration(birth.toLocalTime()), birth.toLocalTime(), birth, birth);
        }

        private Entity(
            @Nullable Long id, @Nullable Year birthYear, @Nullable LocalDate birthDay, @Nullable Duration birthDuration,
            @Nullable LocalTime birthTime, @Nullable LocalDateTime birthDatetime, @Nullable LocalDateTime birthTimestamp
        ) {
            this.id = requireNonNull(id, "id must not be null");
            this.birthYear = requireNonNull(birthYear, "birthYear must not be null");
            this.birthDay = requireNonNull(birthDay, "birthDay must not be null");
            this.birthDuration = requireNonNull(birthDuration, "birthDuration must not be null");
            this.birthTime = requireNonNull(birthTime, "birthTime must not be null");
            this.birthDatetime = requireNonNull(birthDatetime, "birthDatetime must not be null");
            this.birthTimestamp = requireNonNull(birthTimestamp, "birthTimestamp must not be null");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Entity)) {
                return false;
            }
            Entity that = (Entity) o;
            return id == that.id &&
                birthYear.equals(that.birthYear) &&
                birthDay.equals(that.birthDay) &&
                birthDuration.equals(that.birthDuration) &&
                birthTime.equals(that.birthTime) &&
                birthDatetime.equals(that.birthDatetime) &&
                birthTimestamp.equals(that.birthTimestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, birthYear, birthDay, birthDuration, birthTime, birthDatetime, birthTimestamp);
        }

        @Override
        public String toString() {
            return String.format("DateTimeEntity{id=%d, birthYear=%s, birthDay=%s, birthDuration=%s, birthTime=%s, birthDatetime=%s, birthTimestamp=%s}",
                id, birthYear, birthDay, birthDuration, birthTime, birthDatetime, birthTimestamp);
        }

        private static Flux<Entity> from(Result result) {
            return Flux.from(result.map((row, metadata) -> new Entity(
                row.get("id", Long.class),
                row.get("birth_year", Year.class),
                row.get("birth_day", LocalDate.class),
                row.get("birth_time", Duration.class),
                row.get("birth_time", LocalTime.class),
                row.get("birth_datetime", LocalDateTime.class),
                row.get("birth_timestamp", LocalDateTime.class)
            )));
        }

        private static String simpleUpdate(LocalDateTime thirdBirth) {
            return String.format("UPDATE test SET birth_year=%d,birth_day='%s',birth_time='%s',birth_datetime='%s',birth_timestamp='%s' WHERE id > 0",
                thirdBirth.getYear(),
                DateTimeFormatter.ISO_LOCAL_DATE.format(thirdBirth),
                TIME_FORMATTER.format(thirdBirth),
                DATE_TIME_FORMATTER.format(thirdBirth),
                DATE_TIME_FORMATTER.format(thirdBirth)
            );
        }

        private static String simpleInsert(LocalDateTime birth) {
            return String.format("INSERT INTO test VALUES(DEFAULT,%d,'%s','%s','%s','%s')",
                birth.getYear(),
                DateTimeFormatter.ISO_LOCAL_DATE.format(birth),
                TIME_FORMATTER.format(birth),
                DATE_TIME_FORMATTER.format(birth),
                DATE_TIME_FORMATTER.format(birth)
            );
        }

        private static Duration toDuration(LocalTime time) {
            return Duration.ofSeconds(TimeUnit.HOURS.toSeconds(time.getHour()) + TimeUnit.MINUTES.toSeconds(time.getMinute()) + time.getSecond(), time.getNano());
        }

        private static LocalTime toLocalTime(Duration duration) {
            Duration abs = duration.abs();
            long seconds = abs.getSeconds() - TimeUnit.DAYS.toSeconds(abs.toDays());
            int nano = abs.getNano();
            int hour = (int) TimeUnit.SECONDS.toHours(seconds);
            seconds -= TimeUnit.HOURS.toSeconds(hour);
            int minute = (int) TimeUnit.SECONDS.toMinutes(seconds);
            seconds -= TimeUnit.MINUTES.toSeconds(minute);

            return LocalTime.of(hour, minute, (int) seconds, nano);
        }

        private static Duration removeNano(Duration duration) {
            long second = duration.getSeconds();

            if (second < 0 && duration.getNano() > 0) {
                return Duration.ofSeconds(second + 1);
            }
            return duration.withNanos(0);
        }
    }
}
