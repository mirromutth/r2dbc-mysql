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

package dev.miku.r2dbc.mysql;

import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.core.type.TypeReference;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Base class considers data integration unit tests in prepare query for implementations of {@link QueryIntegrationTestSupport}.
 */
abstract class PrepareQueryIntegrationTestSupport extends QueryIntegrationTestSupport {

    PrepareQueryIntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        super(configuration);
    }

    /**
     * See https://github.com/mirromutth/r2dbc-mysql/issues/50 .
     */
    @Test
    void multiQueries() {
        String tdl = "CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,email VARCHAR(190),password VARCHAR(190),updated_at DATETIME,created_at DATETIME)";
        connectionFactory.create()
            .flatMap(connection -> Mono.from(connection.createStatement(tdl)
                .execute())
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .thenMany(Flux.range(0, 10)
                    .flatMap(it -> Flux.from(connection.createStatement("INSERT INTO test VALUES(DEFAULT,?,?,NOW(),NOW())")
                        .bind(0, String.format("integration-test%d@mail.com", it))
                        .bind(1, "******")
                        .execute())
                        .flatMap(IntegrationTestSupport::extractRowsUpdated)))
                .onErrorResume(e -> close(connection).then(Mono.error(e)))
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

        testTypeRef(String.class, "SET('ONE','TWO','THREE')", true, null, "ONE,TWO,THREE", "ONE", "", "ONE,THREE");
        testTypeRef(String[].class, "SET('ONE','TWO','THREE')", true, null, new String[]{"ONE", "TWO", "THREE"}, new String[]{"ONE"}, new String[]{}, new String[]{"ONE", "THREE"});
        testTypeRef(stringSet, "SET('ONE','TWO','THREE')", true, null, new HashSet<>(Arrays.asList("ONE", "TWO", "THREE")), Collections.singleton("ONE"), Collections.emptySet(),
            new HashSet<>(Arrays.asList("ONE", "THREE")));
        testTypeRef(enumSet, "SET('ONE','TWO','THREE')", true, null, EnumSet.allOf(EnumData.class), EnumSet.of(EnumData.ONE), EnumSet.noneOf(EnumData.class), EnumSet.of(EnumData.ONE, EnumData.THREE));
    }

    @Test
    @Override
    void date() {
        testTypeRef(LocalDate.class, "DATE", true, null, MIN_DATE, MAX_DATE);
    }

    @Test
    @Override
    void time() {
        testTypeRef(LocalTime.class, "TIME", true, null, MIN_TIME, MAX_TIME);
        testTypeRef(Duration.class, "TIME", true, null, MIN_DURATION, MAX_DURATION);
    }

    @Override
    Mono<Void> testTimeDuration(Connection connection, Duration origin, LocalTime time) {
        return Mono.from(connection.createStatement("INSERT INTO test VALUES(DEFAULT,?)")
            .bind(0, origin)
            .returnGeneratedValues("id")
            .execute())
            .flatMapMany(QueryIntegrationTestSupport::extractId)
            .concatMap(id -> connection.createStatement("SELECT value FROM test WHERE id=?")
                .bind(0, id)
                .execute())
            .<Optional<LocalTime>>flatMap(r -> extractOptionalField(r, LocalTime.class))
            .map(Optional::get)
            .doOnNext(t -> assertEquals(t, time))
            .then(Mono.from(connection.createStatement("DELETE FROM test WHERE id>0")
                .execute()))
            .flatMap(QueryIntegrationTestSupport::extractRowsUpdated)
            .then();
    }

    @Test
    @Override
    void dateTime() {
        testTypeRef(LocalDateTime.class, "DATETIME", true, null, MIN_DATE_TIME, MAX_DATE_TIME);
    }

    @Test
    @Override
    void timestamp() {
        // TIMESTAMP must not be null when database version less than 8.0
        testTypeRef(LocalDateTime.class, "TIMESTAMP", true, MIN_TIMESTAMP, MAX_TIMESTAMP);
    }

    @Test
    @Override
    void varbinary() {
        testTypeRef(byte[].class, "VARBINARY(50)", true, new byte[0], null, new byte[]{1, 2, 3, 4, 5});
        testTypeRef(ByteBuffer.class, "VARBINARY(50)", true, ByteBuffer.allocate(0), null, ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));
    }

    @Test
    @Override
    void bit() {
        testTypeRef(Boolean.class, "BIT(1)", true, null, false, true);
        testTypeRef(byte[].class, "BIT(16)", false, null, new byte[]{(byte) 0xCD, (byte) 0xEF});
        testTypeRef(BitSet.class, "BIT(16)", false, BitSet.valueOf(new byte[0]), null, BitSet.valueOf(new byte[]{(byte) 0xEF, (byte) 0xCD}));
        testTypeRef(ByteBuffer.class, "BIT(16)", false, null, ByteBuffer.wrap(new byte[]{1, 2}));
    }

    @Test
    @Override
    void consumePartially() {
        connectionFactory.create()
            .flatMapMany(connection -> Mono.from(connection.createStatement("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value INT)")
                .execute())
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .then(Mono.from(connection.createStatement("INSERT INTO test(`value`) VALUES (1),(2),(3),(4),(5)").execute()))
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .then(Mono.from(connection.createStatement("SELECT value FROM test WHERE id > ?")
                    .bind(0, 0)
                    .execute()))
                .flatMapMany(r -> r.map((row, metadata) -> row.get(0, Integer.TYPE))).take(3)
                .concatWith(Mono.from(connection.createStatement("SELECT value FROM test WHERE id > ?")
                    .bind(0, 0)
                    .execute())
                    .flatMapMany(r -> r.map((row, metadata) -> row.get(0, Integer.TYPE))).take(2))
                .concatWith(close(connection))
            )
            .as(StepVerifier::create)
            .expectNext(1, 2, 3, 1, 2)
            .verifyComplete();
    }

    @Test
    @Override
    void ignoreResult() {
        connectionFactory.create()
            .flatMapMany(connection -> Mono.from(connection.createStatement("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value INT)")
                .execute())
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .then(Mono.from(connection.createStatement("INSERT INTO test(`value`) VALUES (1),(2),(3),(4),(5)").execute()))
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .then(Mono.from(connection.createStatement("SELECT value FROM test WHERE id > ?").bind(0, 0).execute()))
                .then(Mono.from(connection.createStatement("SELECT value FROM test ORDER BY id DESC LIMIT ?,?")
                    .bind(0, 2)
                    .bind(1, 5)
                    .execute()))
                .flatMapMany(r -> r.map((row, metadata) -> row.get(0, Integer.TYPE)))
                .concatWith(close(connection))
            )
            .as(StepVerifier::create)
            .expectNext(3, 2, 1)
            .verifyComplete();
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    @Override
    final <T> void testType(Class<T> type, String defined, boolean ignored, T... values) {
        // Should use simple statement for table definition.
        testTypeRef(type, defined, true, values);
    }

    void testJson(String... values) {
        // The value of JSON cannot be a condition.
        testTypeRef(String.class, "JSON", false, values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final <T> void testTypeRef(Type type, String defined, boolean valueSelect, T... values) {
        // Should use simple statement for table definition.
        connectionFactory.create()
            .flatMap(connection -> Mono.from(connection.createStatement(String.format("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value %s)", defined))
                .execute())
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .thenMany(Flux.fromIterable(convertOptional(values)).concatMap(value -> testOne(connection, type, valueSelect, value.orElse(null))))
                .concatWith(close(connection))
                .then())
            .as(StepVerifier::create)
            .verifyComplete();
    }

    private static <T> List<Optional<T>> convertOptional(T[] values) {
        List<Optional<T>> optionals = new ArrayList<>(values.length);

        for (T value : values) {
            optionals.add(Optional.ofNullable(value));
        }

        return optionals;
    }

    private static <T> Mono<Void> testOne(MySqlConnection connection, Type type, boolean selectValue, @Nullable T value) {
        MySqlStatement insert = connection.createStatement("INSERT INTO test VALUES(DEFAULT,?)");

        if (value == null) {
            if (type instanceof Class<?>) {
                insert.bindNull(0, (Class<?>) type);
            } else {
                insert.bindNull(0, (Class<?>) ((ParameterizedType) type).getRawType());
            }
        } else {
            if (value instanceof ByteBuffer) {
                insert.bind(0, ((ByteBuffer) value).slice());
            } else {
                insert.bind(0, value);
            }
        }

        return Mono.from(insert.returnGeneratedValues("id")
            .execute())
            .flatMap(result -> extractRowsUpdated(result)
                .doOnNext(u -> assertEquals(u, 1))
                .thenMany(extractId(result))
                .collectList()
                .map(ids -> {
                    assertEquals(1, ids.size());
                    return ids.get(0);
                }))
            .flatMap(id -> Mono.from(connection.createStatement("SELECT value FROM test WHERE id=?")
                .bind(0, id)
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
                            assertArrayEquals((byte[]) t, (byte[]) value);
                        } else {
                            assertArrayEquals((Object[]) t, (Object[]) value);
                        }
                    } else {
                        assertEquals(t, value);
                    }
                } else {
                    assertNull(value);
                }
            })
            .as(it -> {
                if (selectValue) {
                    MySqlStatement valueSelect;

                    if (value == null) {
                        valueSelect = connection.createStatement("SELECT value FROM test WHERE value IS NULL");
                    } else {
                        valueSelect = connection.createStatement("SELECT value FROM test WHERE value=?");

                        if (value instanceof ByteBuffer) {
                            valueSelect.bind(0, ((ByteBuffer) value).slice());
                        } else {
                            valueSelect.bind(0, value);
                        }
                    }

                    return it.then(Mono.from(valueSelect.execute()))
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
                                        assertArrayEquals((byte[]) t, (byte[]) value);
                                    } else {
                                        assertArrayEquals((Object[]) t, (Object[]) value);
                                    }
                                } else {
                                    assertEquals(t, value);
                                }
                            } else {
                                assertNull(value);
                            }
                        });
                }
                return it;
            })
            .then(Mono.from(connection.createStatement("DELETE FROM test WHERE id>0").execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .doOnNext(u -> assertEquals(u, 1))
            .then();
    }
}
