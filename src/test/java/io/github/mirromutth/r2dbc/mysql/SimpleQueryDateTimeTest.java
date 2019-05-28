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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.concurrent.ThreadLocalRandom;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for CRUD datetime on simple query.
 */
class SimpleQueryDateTimeTest {

    private static final String TABLE_DDL = "CREATE TABLE `birth` (\n" +
        "  `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n" +
        "  `birth_year` YEAR NOT NULL,\n" +
        "  `birth_day` DATE NOT NULL DEFAULT '2000-01-01',\n" +
        "  `birth_time` TIME NOT NULL,\n" +
        "  `birth_datetime` DATETIME NOT NULL DEFAULT '1970-01-01 08:00:00',\n" +
        "  `birth_timestamp` TIMESTAMP NOT NULL\n" +
        ");\n";

    // Make sure that all date time is not equal.
    private static final LocalDateTime BIRTH_FIRST = randomDateTime(1971, 2000);

    private static final LocalDateTime BIRTH_SECOND = randomDateTime(2000, 2020);

    // Avoid the 2038 problem.
    private static final LocalDateTime BIRTH_THIRD = randomDateTime(2020, 2038);

    private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .toFormatter();

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .appendLiteral(' ')
        .append(TIME_FORMATTER)
        .toFormatter();

    private static final MySqlConnectionRunner RUNNER = MySqlConnectionRunner.ofVersion(5, 7);

    @BeforeAll
    static void createTable() throws Throwable {
        RUNNER.run(Duration.ofSeconds(16), connection -> connection.createStatement(TABLE_DDL)
            .execute()
            .flatMap(MySqlResult::getRowsUpdated)
        );
    }

    @AfterAll
    static void dropTable() throws Throwable {
        RUNNER.run(Duration.ofSeconds(16), connection -> connection.createStatement("DROP TABLE `birth`")
            .execute()
            .flatMap(MySqlResult::getRowsUpdated)
        );
    }

    @Test
    void crudAllInOne() throws Throwable {
        RUNNER.run(Duration.ofSeconds(24), connection -> {
            MySqlStatement insertFirstStmt = connection.createStatement(formattedInsert(BIRTH_FIRST));
            MySqlStatement insertSecondStmt = connection.createStatement(formattedInsert(BIRTH_SECOND));
            MySqlStatement selectStmt = connection.createStatement("SELECT * FROM `birth` ORDER BY `id`");
            MySqlStatement updateStmt = connection.createStatement(formattedUpdate());
            MySqlStatement deleteStmt = connection.createStatement("DELETE FROM `birth` WHERE `id` > 0");

            return insertFirstStmt.execute()
                .flatMap(MySqlResult::getRowsUpdated)
                .doOnNext(u -> assertEquals(u.intValue(), 1))
                .then(insertSecondStmt.execute())
                .flatMap(MySqlResult::getRowsUpdated)
                .doOnNext(u -> assertEquals(u.intValue(), 1))
                .then(selectStmt.execute())
                .flatMapMany(Entity::from)
                .collectList()
                .doOnNext(entities -> {
                    assertEquals(entities.size(), 2);
                    assertEquals(entities.get(0), toEntity(1L, BIRTH_FIRST));
                    assertEquals(entities.get(1), toEntity(2L, BIRTH_SECOND));
                })
                .then(updateStmt.execute())
                .flatMap(MySqlResult::getRowsUpdated)
                .doOnNext(u -> assertEquals(u.intValue(), 2))
                .then(selectStmt.execute())
                .flatMapMany(Entity::from)
                .collectList()
                .doOnNext(entities -> {
                    assertEquals(entities.size(), 2);
                    assertEquals(entities.get(0), toEntity(1L, BIRTH_THIRD));
                    assertEquals(entities.get(1), toEntity(2L, BIRTH_THIRD));
                })
                .then(deleteStmt.execute())
                .flatMap(MySqlResult::getRowsUpdated)
                .doOnNext(u -> assertEquals(u.intValue(), 2));
        });
    }

    private static String formattedUpdate() {
        return String.format("UPDATE `birth` SET\n" +
                "`birth_year` = %d,\n" +
                "`birth_day` = '%s',\n" +
                "`birth_time` = '%s',\n" +
                "`birth_datetime` = '%s',\n" +
                "`birth_timestamp` = '%s' WHERE `id` > 0",
            BIRTH_THIRD.getYear(),
            DateTimeFormatter.ISO_LOCAL_DATE.format(BIRTH_THIRD),
            TIME_FORMATTER.format(BIRTH_THIRD),
            DATE_TIME_FORMATTER.format(BIRTH_THIRD),
            DATE_TIME_FORMATTER.format(BIRTH_THIRD)
        );
    }

    private static String formattedInsert(LocalDateTime birth) {
        return String.format("INSERT INTO `birth` " +
                "(`birth_year`, `birth_day`, `birth_time`, `birth_datetime`, `birth_timestamp`) " +
                "VALUES (%d, '%s', '%s', '%s', '%s')",
            birth.getYear(),
            DateTimeFormatter.ISO_LOCAL_DATE.format(birth),
            TIME_FORMATTER.format(birth),
            DATE_TIME_FORMATTER.format(birth),
            DATE_TIME_FORMATTER.format(birth)
        );
    }

    private static LocalDateTime randomDateTime(int originYear, int boundYear) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        return LocalDateTime.of(
            random.nextInt(originYear, boundYear),
            random.nextInt(1, 13),
            random.nextInt(1, 29),
            random.nextInt(0, 24),
            random.nextInt(0, 60),
            random.nextInt(0, 60)
        );
    }

    private static Entity toEntity(long id, LocalDateTime birth) {
        return new Entity(id, Year.of(birth.getYear()), birth.toLocalDate(), birth.toLocalTime(), birth, birth);
    }

    private static final class Entity {

        private final long id;

        private final Year birthYear;

        private final LocalDate birthDay;

        private final LocalTime birthTime;

        private final LocalDateTime birthDatetime;

        private final LocalDateTime birthTimestamp;

        private Entity(
            @Nullable Long id,
            @Nullable Year birthYear,
            @Nullable LocalDate birthDay,
            @Nullable LocalTime birthTime,
            @Nullable LocalDateTime birthDatetime,
            @Nullable LocalDateTime birthTimestamp
        ) {
            this.id = requireNonNull(id, "id must not be null");
            this.birthYear = requireNonNull(birthYear, "birthYear must not be null");
            this.birthDay = requireNonNull(birthDay, "birthDay must not be null");
            this.birthTime = requireNonNull(birthTime, "birthTime must not be null");
            this.birthDatetime = requireNonNull(birthDatetime, "birthDatetime must not be null");
            this.birthTimestamp = requireNonNull(birthTimestamp, "birthTimestamp must not be null");
        }

        private static Flux<Entity> from(MySqlResult result) {
            return result.map((row, metadata) -> new Entity(
                row.get("id", long.class),
                row.get("birth_year", Year.class),
                row.get("birth_day", LocalDate.class),
                row.get("birth_time", LocalTime.class),
                row.get("birth_datetime", LocalDateTime.class),
                row.get("birth_timestamp", LocalDateTime.class)
            ));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Entity)) {
                return false;
            }

            Entity entity = (Entity) o;

            if (id != entity.id) {
                return false;
            }
            if (!birthYear.equals(entity.birthYear)) {
                return false;
            }
            if (!birthDay.equals(entity.birthDay)) {
                return false;
            }
            if (!birthTime.equals(entity.birthTime)) {
                return false;
            }
            if (!birthDatetime.equals(entity.birthDatetime)) {
                return false;
            }
            return birthTimestamp.equals(entity.birthTimestamp);
        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + birthYear.hashCode();
            result = 31 * result + birthDay.hashCode();
            result = 31 * result + birthTime.hashCode();
            result = 31 * result + birthDatetime.hashCode();
            result = 31 * result + birthTimestamp.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Entity{" +
                "id=" + id +
                ", birthYear=" + birthYear +
                ", birthDay=" + birthDay +
                ", birthTime=" + birthTime +
                ", birthDatetime=" + birthDatetime +
                ", birthTimestamp=" + birthTimestamp +
                '}';
        }
    }
}
