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
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class considers data integration unit tests in simple query for implementations of {@link IntegrationTestSupport}.
 */
abstract class SimpleQueryIntegrationTestSupport extends IntegrationTestSupport {

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

    SimpleQueryIntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        super(configuration);
    }

    /**
     * SQL query {@code SELECT 1} is a ping approach which is cross-database (maybe some are not
     * supported) and is easy to understand.
     * <p>
     * Note: looks like {@code SELECT 1} result value type returned by the MySQL server is BIGINT,
     * try using Number.class to eliminate {@code assertEquals} fail because of the value type.
     */
    @Test
    void selectOne() {
        Mono.from(connectionFactory.create())
            .flatMap(connection -> Mono.from(connection.createStatement("SELECT 1").execute())
                .flatMapMany(result -> result.map((row, metadata) -> row.get(0, Number.class)))
                .doOnNext(number -> assertEquals(number.intValue(), 1))
                .reduce((x, y) -> Math.addExact(x.intValue(), y.intValue()))
                .doOnNext(number -> assertEquals(number.intValue(), 1))
                .concatWith(close(connection))
                .then())
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    @Override
    void varchar() {
        testType(String.class, "VARCHAR(50)", Functions.STRING, "", null, "data");
    }

    @Test
    @Override
    void date() {
        testType(LocalDate.class, "DATE", Functions.DATE, null, MIN_DATE, MAX_DATE);
    }

    @Test
    @Override
    void time() {
        testType(LocalTime.class, "TIME", Functions.TIME, null, MIN_TIME, MAX_TIME);
        testType(Duration.class, "TIME", Functions.DURATION, null, MIN_DURATION, MAX_DURATION);
    }

    @Override
    Mono<Void> testTime(Connection connection, Duration origin, LocalTime time) {
        String originValue = Functions.DURATION.apply(origin);

        return Mono.from(connection.createStatement(String.format("INSERT INTO test VALUES(DEFAULT,'%s')", originValue))
            .returnGeneratedValues("id")
            .execute())
            .flatMapMany(IntegrationTestSupport::extractId)
            .concatMap(id -> connection.createStatement(String.format("SELECT value FROM test WHERE id=%d", id))
                .execute())
            .flatMap(r -> extractOptionalField(r, LocalTime.class))
            .map(Optional::get)
            .doOnNext(t -> assertEquals(t, time))
            .then(Mono.from(connection.createStatement("DELETE FROM test WHERE id>0")
                .execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then();
    }

    @Test
    @Override
    void dateTime() {
        testType(LocalDateTime.class, "DATETIME", Functions.DATE_TIME, null, MIN_DATE_TIME, MAX_DATE_TIME);
    }

    @Test
    @Override
    void timestamp() {
        // TIMESTAMP must not be null when database version less than 8.0
        testType(LocalDateTime.class, "TIMESTAMP", Functions.DATE_TIME, MIN_TIMESTAMP, MAX_TIMESTAMP);
    }

    @SafeVarargs
    @Override
    protected final <T> void testType(Class<T> type, String defined, T... values) {
        ValueString<?>[] v = new ValueString<?>[values.length];

        for (int i = 0; i < values.length; ++i) {
            v[i] = new ValueString<>(values[i]);
        }

        testType(type, defined, false, v);
    }

    @SafeVarargs
    private final <T> void testType(Class<T> type, String defined, Function<T, String> converter, T... values) {
        ValueString<?>[] v = new ValueString<?>[values.length];

        for (int i = 0; i < values.length; ++i) {
            v[i] = new ValueString<>(values[i], converter);
        }

        testType(type, defined, true, v);
    }

    @SuppressWarnings("varargs")
    private <T> void testType(Class<T> type, String defined, boolean quotation, ValueString<?>... values) {
        connectionFactory.create()
            .flatMap(connection -> {
                Mono<Void> task = Mono.from(connection.createStatement(String.format("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value %s)", defined))
                    .execute())
                    .flatMap(CompatibilityTestSupport::extractRowsUpdated)
                    .then();

                for (ValueString<?> value : values) {
                    task = task.then(testOne(connection, type, value, quotation));
                }

                return task.concatWith(close(connection)).then();
            })
            .as(StepVerifier::create)
            .verifyComplete();
    }

    private static <T> Mono<Void> testOne(MySqlConnection connection, Class<T> type, ValueString<?> value, boolean quotation) {
        String valueString = value.isNull() ? "NULL" : (quotation ? String.format("'%s'", value) : value.toString());
        String valueSelect = value.isNull() ? "SELECT value FROM test WHERE value IS NULL" : String.format("SELECT value FROM test WHERE value=%s", valueString);

        return Mono.from(connection.createStatement(String.format("INSERT INTO test VALUES(DEFAULT,%s)", valueString))
            .returnGeneratedValues("id")
            .execute())
            .flatMap(result -> extractRowsUpdated(result)
                .doOnNext(u -> assertEquals(u, 1))
                .thenMany(extractId(result))
                .collectList()
                .map(ids -> {
                    assertEquals(1, ids.size());
                    return ids.get(0);
                }))
            .flatMap(id -> Mono.from(connection.createStatement(String.format("SELECT value FROM test WHERE id=%d", id))
                .execute()))
            .flatMapMany(r -> extractOptionalField(r, type))
            .collectList()
            .doOnNext(data -> assertEquals(data, Collections.singletonList(Optional.ofNullable(value.getValue()))))
            .then(Mono.from(connection.createStatement(valueSelect)
                .execute()))
            .flatMapMany(r -> extractOptionalField(r, type))
            .collectList()
            .doOnNext(data -> assertEquals(data, Collections.singletonList(Optional.ofNullable(value.getValue()))))
            .then(Mono.from(connection.createStatement("DELETE FROM test WHERE id>0").execute()))
            .flatMap(CompatibilityTestSupport::extractRowsUpdated)
            .doOnNext(u -> assertEquals(u, 1))
            .then();
    }

    private static final class Functions {

        private static final Function<String, String> STRING = Function.identity();

        private static final Function<LocalDate, String> DATE = LocalDate::toString;

        private static final Function<LocalTime, String> TIME = TIME_FORMATTER::format;

        private static final Function<Duration, String> DURATION = duration -> {
            boolean isNegative = duration.isNegative();
            long seconds = duration.abs().getSeconds();
            int hour = (int) TimeUnit.SECONDS.toHours(seconds);
            seconds -= TimeUnit.HOURS.toSeconds(hour);
            int minute = (int) TimeUnit.SECONDS.toMinutes(seconds);
            seconds -= TimeUnit.MINUTES.toSeconds(minute);

            if (isNegative) {
                return String.format("-%02d:%02d:%02d", hour, minute, seconds);
            } else {
                return String.format("%02d:%02d:%02d", hour, minute, seconds);
            }
        };

        private static final Function<LocalDateTime, String> DATE_TIME = DATE_TIME_FORMATTER::format;
    }

    private static class ValueString<T> {

        @Nullable
        private final T value;

        private final String serialized;

        private ValueString(@Nullable T value) {
            this(value, value == null ? "NULL" : value.toString());
        }

        private ValueString(@Nullable T value, Function<T, String> converter) {
            this(value, value == null ? "NULL" : converter.apply(value));
        }

        private ValueString(@Nullable T value, String serialized) {
            this.value = value;
            this.serialized = serialized;
        }

        @Nullable
        T getValue() {
            return value;
        }

        public boolean isNull() {
            return value == null;
        }

        @Override
        public String toString() {
            return serialized;
        }
    }
}
