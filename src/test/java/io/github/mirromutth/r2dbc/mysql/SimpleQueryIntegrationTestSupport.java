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
import org.testcontainers.shaded.com.fasterxml.jackson.core.type.TypeReference;
import org.testcontainers.shaded.org.apache.commons.lang.ArrayUtils;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
    void set() {
        Type stringSet = new TypeReference<Set<String>>() {

        }.getType();
        Type enumSet = new TypeReference<Set<EnumData>>() {

        }.getType();
        String defined = "SET('ONE','TWO','THREE')";

        testType(String.class, defined, true, null, "ONE,TWO,THREE", "ONE", "", "ONE,THREE");
        testTypeRef(String[].class, defined, Functions.STRING_ARRAY, null, new String[]{"ONE", "TWO", "THREE"}, new String[]{"ONE"}, new String[]{}, new String[]{"ONE", "THREE"});
        testTypeRef(stringSet, defined, Functions.SET, null, new HashSet<>(Arrays.asList("ONE", "TWO", "THREE")), Collections.singleton("ONE"), Collections.emptySet(), new HashSet<>(Arrays.asList("ONE", "THREE")));
        testTypeRef(enumSet, defined, Functions.SET, null, EnumSet.allOf(EnumData.class), EnumSet.of(EnumData.ONE), EnumSet.noneOf(EnumData.class), EnumSet.of(EnumData.ONE, EnumData.THREE));
    }

    @Test
    @Override
    void date() {
        testTypeRef(LocalDate.class, "DATE", Functions.DATE, null, MIN_DATE, MAX_DATE);
    }

    @Test
    @Override
    void time() {
        testTypeRef(LocalTime.class, "TIME", Functions.TIME, null, MIN_TIME, MAX_TIME);
        testTypeRef(Duration.class, "TIME", Functions.DURATION, null, MIN_DURATION, MAX_DURATION);
    }

    @Override
    Mono<Void> testTimeDuration(Connection connection, Duration origin, LocalTime time) {
        String originValue = Functions.DURATION.apply(origin);

        return Mono.from(connection.createStatement(String.format("INSERT INTO test VALUES(DEFAULT,'%s')", originValue))
            .returnGeneratedValues("id")
            .execute())
            .flatMapMany(IntegrationTestSupport::extractId)
            .concatMap(id -> connection.createStatement(String.format("SELECT value FROM test WHERE id=%d", id))
                .execute())
            .<Optional<LocalTime>>flatMap(r -> extractOptionalField(r, LocalTime.class))
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
        testTypeRef(LocalDateTime.class, "DATETIME", Functions.DATE_TIME, null, MIN_DATE_TIME, MAX_DATE_TIME);
    }

    @Test
    @Override
    void timestamp() {
        // TIMESTAMP must not be null when database version less than 8.0
        testTypeRef(LocalDateTime.class, "TIMESTAMP", Functions.DATE_TIME, MIN_TIMESTAMP, MAX_TIMESTAMP);
    }

    @Test
    @Override
    void bit() {
        testTypeQuota(Boolean.class, "BIT(1)", Functions.BOOLEAN, false, null, false, true);
        testTypeQuota(byte[].class, "BIT(16)", Functions.BYTE_ARRAY, false, null, new byte[]{(byte) 0xCD, (byte) 0xEF});
        testTypeQuota(BitSet.class, "BIT(16)", Functions.BIT_SET, false, null, BitSet.valueOf(new byte[]{(byte) 0xEF, (byte) 0xCD}));
    }

    @SafeVarargs
    @Override
    final <T> void testType(Class<T> type, String defined, boolean quota, T... values) {
        ValueString<?>[] v = new ValueString<?>[values.length];

        for (int i = 0; i < values.length; ++i) {
            v[i] = new ValueString<>(values[i]);
        }

        testType(type, defined, true, quota, v);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final <T> void testTypeRef(Type type, String defined, Function<T, String> converter, T... values) {
        testTypeQuota(type, defined, converter, true, values);
    }

    @SafeVarargs
    private final <T> void testTypeQuota(Type type, String defined, Function<T, String> converter, boolean quota, T... values) {
        ValueString<?>[] v = new ValueString<?>[values.length];

        for (int i = 0; i < values.length; ++i) {
            v[i] = new ValueString<>(values[i], converter);
        }

        testType(type, defined, true, quota, v);
    }

    void testJson(String... values) {
        ValueString<?>[] v = new ValueString<?>[values.length];

        for (int i = 0; i < values.length; ++i) {
            v[i] = new ValueString<>(values[i]);
        }

        // The value of JSON cannot be a condition
        testType(String.class, "JSON", false, true, v);
    }

    @SuppressWarnings("varargs")
    private void testType(Type type, String defined, boolean selectValue, boolean quota, ValueString<?>[] values) {
        connectionFactory.create()
            .flatMap(connection -> {
                Mono<Void> task = Mono.from(connection.createStatement(String.format("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value %s)", defined))
                    .execute())
                    .flatMap(CompatibilityTestSupport::extractRowsUpdated)
                    .then();

                for (ValueString<?> value : values) {
                    task = task.then(testOne(connection, type, value, selectValue, quota));
                }

                return task.concatWith(close(connection)).then();
            })
            .as(StepVerifier::create)
            .verifyComplete();
    }

    private static <T> Mono<Void> testOne(MySqlConnection connection, Type type, ValueString<?> value, boolean selectValue, boolean quota) {
        String valueString = value.isNull() ? "NULL" : (quota ? String.format("'%s'", value) : value.toString());

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
            .<Optional<T>>flatMapMany(r -> extractOptionalField(r, type))
            .collectList()
            .map(list -> {
                assertEquals(list.size(), 1);
                return list.get(0);
            })
            .doOnNext(data -> {
                if (data.isPresent()) {
                    T t = data.get();
                    Class<?> clazz = t.getClass();
                    if (clazz.isArray()) {
                        if (clazz == byte[].class) {
                            assertArrayEquals((byte[]) t, (byte[]) value.value);
                        } else {
                            assertArrayEquals((Object[]) t, (Object[]) value.value);
                        }
                    } else {
                        assertEquals(t, value.value);
                    }
                } else {
                    assertNull(value.value);
                }
            })
            .as(it -> {
                if (selectValue) {
                    String valueSelect = value.isNull() ? "SELECT value FROM test WHERE value IS NULL" : String.format("SELECT value FROM test WHERE value=%s", valueString);

                    return it.then(Mono.from(connection.createStatement(valueSelect)
                        .execute()))
                        .<Optional<T>>flatMapMany(r -> extractOptionalField(r, type))
                        .collectList()
                        .map(list -> {
                            assertEquals(list.size(), 1);
                            return list.get(0);
                        })
                        .doOnNext(data -> {
                            if (data.isPresent()) {
                                T t = data.get();
                                Class<?> clazz = t.getClass();
                                if (clazz.isArray()) {
                                    if (clazz == byte[].class) {
                                        assertArrayEquals((byte[]) t, (byte[]) value.value);
                                    } else {
                                        assertArrayEquals((Object[]) t, (Object[]) value.value);
                                    }
                                } else {
                                    assertEquals(t, value.value);
                                }
                            } else {
                                assertNull(value.value);
                            }
                        });
                }
                return it;
            })
            .then(Mono.from(connection.createStatement("DELETE FROM test WHERE id>0").execute()))
            .flatMap(CompatibilityTestSupport::extractRowsUpdated)
            .doOnNext(u -> assertEquals(u, 1))
            .then();
    }

    private static final class Functions {

        private static final Function<String[], String> STRING_ARRAY = v -> String.join(",", v);

        private static final Function<Set<?>, String> SET = s -> s.stream().map(Object::toString).collect(Collectors.joining(","));

        private static final Function<LocalDate, String> DATE = LocalDate::toString;

        private static final Function<LocalTime, String> TIME = TIME_FORMATTER::format;

        private static final Function<Boolean, String> BOOLEAN = b -> b ? "b'1'" : "b'0'";

        private static final Function<byte[], String> BYTE_ARRAY = bytes -> IntStream.range(0, bytes.length).mapToObj(idx -> {
            byte b = bytes[idx];
            return String.format("%d%d%d%d%d%d%d%d", (b >>> 7) & 1, (b >>> 6) & 1, (b >>> 5) & 1, (b >>> 4) & 1, (b >>> 3) & 1, (b >>> 2) & 1, (b >>> 1) & 1, b & 1);
        }).collect(Collectors.joining("", "b'", "'"));

        private static final Function<BitSet, String> BIT_SET = b -> {
            byte[] bytes = b.toByteArray();
            ArrayUtils.reverse(bytes);
            return BYTE_ARRAY.apply(bytes);
        };

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

        public boolean isNull() {
            return value == null;
        }

        @Override
        public String toString() {
            return serialized;
        }
    }
}
