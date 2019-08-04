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
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.test.Example;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Extra unit tests for integers which append to TCK.
 */
interface MySqlExampleIntsExtra extends MySqlExampleExtra {

    @Test
    default void intsPreparedCrud() {
        long firstId = 6L;

        getJdbcOperations().execute(String.format("DROP TABLE %s", "test"));
        getJdbcOperations().execute(String.format("CREATE TABLE test(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY," +
            "data_int8 TINYINT,data_uint8 TINYINT UNSIGNED," +
            "data_int16 SMALLINT,data_uint16 SMALLINT UNSIGNED," +
            "data_int24 MEDIUMINT,data_uint24 MEDIUMINT UNSIGNED," +
            "data_int32 INT,data_uint32 INT UNSIGNED," +
            "data_int64 BIGINT,data_uint64 BIGINT UNSIGNED)AUTO_INCREMENT=%d", firstId));

        int incrementStep = 5;
        IntsEntity allNull = new IntsEntity(firstId, null, null, null, null, null, null, null, null, null, null);
        IntsEntity oddHalfNull = new IntsEntity(firstId + incrementStep, null, (short) 11, null, 22, null, 33, null, 44L, null, BigInteger.valueOf(55));
        IntsEntity evenHalfNull = new IntsEntity(firstId + incrementStep * 2, (byte) -11, null, (short) -22, null, -33, null, -44, null, -44L, null);
        IntsEntity full = new IntsEntity(firstId + incrementStep * 3, (byte) -66, (short) 77, (short) 88, 99, -1010, 1111, 1212, 1313L, -1414L, BigInteger.valueOf(1515));
        List<IntsEntity> odds = IntsEntity.selectOdd(true, allNull, oddHalfNull, evenHalfNull, full);
        List<IntsEntity> evens = IntsEntity.selectOdd(false, allNull, oddHalfNull, evenHalfNull, full);

        Mono.from(getConnectionFactory().create())
            .flatMap(connection -> Mono.from(connection.createStatement("SET @@auto_increment_increment=?")
                .bind(0, (Integer) incrementStep)
                .execute())
                .flatMap(Example::extractRowsUpdated)
                .thenMany(IntsEntity.createPreparedInsert(connection, allNull, oddHalfNull, evenHalfNull, full).execute())
                .flatMap(result -> Example.extractRowsUpdated(result)
                    .doOnNext(u -> assertEquals(u.intValue(), 1))
                    .thenMany(result.map((row, metadata) -> row.get("generated_id", Long.class))))
                .collectList()
                .doOnNext(ids -> assertEquals(ids, Arrays.asList(firstId, firstId + incrementStep, firstId + incrementStep * 2, firstId + incrementStep * 3)))
                .thenMany(connection.createStatement("SELECT * FROM test WHERE id % 2 = ?is_odd")
                    .bind("is_odd", true)
                    .add()
                    .bind(0, false)
                    .execute())
                .flatMap(result -> IntsEntity.from(result).collectList())
                .collectList()
                .doOnNext(entities -> assertEquals(entities, Arrays.asList(odds, evens)))
                .thenMany(connection.createStatement("DELETE FROM test WHERE id % 2 = ?is_odd")
                    .bind(0, true)
                    .add()
                    .bind("is_odd", false)
                    .execute())
                .flatMap(Example::extractRowsUpdated)
                .collectList()
                .doOnNext(rowsUpdated -> assertEquals(rowsUpdated, Arrays.asList(2, 2)))
                .concatWith(Example.close(connection))
                .then())
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    default void intsSimpleCrud() {
        long firstId = 6L;

        getJdbcOperations().execute(String.format("DROP TABLE %s", "test"));
        getJdbcOperations().execute(String.format("CREATE TABLE test(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY," +
            "data_int8 TINYINT,data_uint8 TINYINT UNSIGNED," +
            "data_int16 SMALLINT,data_uint16 SMALLINT UNSIGNED," +
            "data_int24 MEDIUMINT,data_uint24 MEDIUMINT UNSIGNED," +
            "data_int32 INT,data_uint32 INT UNSIGNED," +
            "data_int64 BIGINT,data_uint64 BIGINT UNSIGNED)AUTO_INCREMENT=%d", firstId));

        int incrementStep = 5;
        IntsEntity allNull = new IntsEntity(firstId, null, null, null, null, null, null, null, null, null, null);
        IntsEntity oddHalfNull = new IntsEntity(firstId + incrementStep, null, (short) 11, null, 22, null, 33, null, 44L, null, BigInteger.valueOf(55));
        IntsEntity evenHalfNull = new IntsEntity(firstId + incrementStep * 2, (byte) -11, null, (short) -22, null, -33, null, -44, null, -44L, null);
        IntsEntity full = new IntsEntity(firstId + incrementStep * 3, (byte) -66, (short) 77, (short) 88, 99, -1010, 1111, 1212, 1313L, -1414L, BigInteger.valueOf(1515));
        List<IntsEntity> odds = IntsEntity.selectOdd(true, allNull, oddHalfNull, evenHalfNull, full);
        List<IntsEntity> evens = IntsEntity.selectOdd(false, allNull, oddHalfNull, evenHalfNull, full);

        Mono.from(getConnectionFactory().create())
            .flatMap(connection -> Mono.from(connection.createStatement(String.format("SET @@auto_increment_increment=%d", incrementStep))
                .execute())
                .flatMap(Example::extractRowsUpdated)
                .thenMany(IntsEntity.createSimpleInsert(connection, allNull, oddHalfNull, evenHalfNull, full)
                    .execute())
                .flatMap(result -> Example.extractRowsUpdated(result)
                    .doOnNext(u -> assertEquals(u, 4))
                    .thenMany(result.map((row, metadata) -> row.get("generated_id", Long.class))))
                .collectList()
                .doOnNext(ids -> assertEquals(ids, Collections.singletonList(firstId)))
                .thenMany(connection.createStatement("SELECT * FROM test WHERE id % 2 = true; SELECT * FROM test WHERE id % 2 = false")
                    .execute())
                .flatMap(result -> IntsEntity.from(result).collectList())
                .collectList()
                .doOnNext(entities -> assertEquals(entities, Arrays.asList(odds, evens)))
                .thenMany(connection.createStatement("DELETE FROM test WHERE id % 2 = true; DELETE FROM test WHERE id % 2 = false")
                    .execute())
                .flatMap(Example::extractRowsUpdated)
                .collectList()
                .doOnNext(rowsUpdated -> assertEquals(rowsUpdated, Arrays.asList(2, 2)))
                .concatWith(Example.close(connection))
                .then()
            )
            .as(StepVerifier::create)
            .verifyComplete();
    }

    final class IntsEntity {

        private final long id;

        @Nullable
        private final Byte int8;

        @Nullable
        private final Short uint8;

        @Nullable
        private final Short int16;

        @Nullable
        private final Integer uint16;

        @Nullable
        private final Integer int24;

        @Nullable
        private final Integer uint24;

        @Nullable
        private final Integer int32;

        @Nullable
        private final Long uint32;

        @Nullable
        private final Long int64;

        @Nullable
        private final BigInteger uint64;

        private IntsEntity(
            @Nullable Long id, @Nullable Byte int8, @Nullable Short uint8, @Nullable Short int16, @Nullable Integer uint16,
            @Nullable Integer int24, @Nullable Integer uint24, @Nullable Integer int32, @Nullable Long uint32,
            @Nullable Long int64, @Nullable BigInteger uint64
        ) {
            this.id = requireNonNull(id, "id must not be null");
            this.int8 = int8;
            this.uint8 = uint8;
            this.int16 = int16;
            this.uint16 = uint16;
            this.int24 = int24;
            this.uint24 = uint24;
            this.int32 = int32;
            this.uint32 = uint32;
            this.int64 = int64;
            this.uint64 = uint64;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IntsEntity)) {
                return false;
            }
            IntsEntity that = (IntsEntity) o;
            return id == that.id &&
                Objects.equals(int8, that.int8) &&
                Objects.equals(uint8, that.uint8) &&
                Objects.equals(int16, that.int16) &&
                Objects.equals(uint16, that.uint16) &&
                Objects.equals(int24, that.int24) &&
                Objects.equals(uint24, that.uint24) &&
                Objects.equals(int32, that.int32) &&
                Objects.equals(uint32, that.uint32) &&
                Objects.equals(int64, that.int64) &&
                Objects.equals(uint64, that.uint64);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, int8, uint8, int16, uint16, int24, uint24, int32, uint32, int64, uint64);
        }

        @Override
        public String toString() {
            return String.format("IntsEntity{id=%d, int8=%s, uint8=%s, int16=%s, uint16=%d, int24=%d, uint24=%d, int32=%d, uint32=%d, int64=%d, uint64=%s}",
                id, int8, uint8, int16, uint16, int24, uint24, int32, uint32, int64, uint64);
        }

        private String values() {
            return String.format("(DEFAULT,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                intValue(int8), intValue(uint8),
                intValue(int16), intValue(uint16),
                intValue(int24), intValue(uint24),
                intValue(int32), intValue(uint32),
                intValue(int64), intValue(uint64));
        }

        private static Object intValue(@Nullable Number number) {
            return number == null ? "NULL" : number;
        }

        private void bind(Statement statement) {
            if (int8 != null) {
                statement.bind(0, int8);
            } else {
                statement.bindNull(0, Byte.class);
            }

            if (uint8 != null) {
                statement.bind(1, uint8);
            } else {
                statement.bindNull(1, Short.class);
            }

            if (int16 != null) {
                statement.bind(2, int16);
            } else {
                statement.bindNull(2, Short.class);
            }

            if (uint16 != null) {
                statement.bind(3, uint16);
            } else {
                statement.bindNull(3, Integer.class);
            }

            if (int24 != null) {
                statement.bind(4, int24);
            } else {
                statement.bindNull(4, Integer.class);
            }

            if (uint24 != null) {
                statement.bind(5, uint24);
            } else {
                statement.bindNull(5, Integer.class);
            }

            if (int32 != null) {
                statement.bind(6, int32);
            } else {
                statement.bindNull(6, Integer.class);
            }

            if (uint32 != null) {
                statement.bind(7, uint32);
            } else {
                statement.bindNull(7, Long.class);
            }

            if (int64 != null) {
                statement.bind(8, int64);
            } else {
                statement.bindNull(8, Long.class);
            }

            if (uint64 != null) {
                statement.bind(9, uint64);
            } else {
                statement.bindNull(9, BigInteger.class);
            }

            statement.add();
        }

        private static Flux<IntsEntity> from(Result result) {
            return Flux.from(result.map((row, metadata) -> new IntsEntity(
                row.get(0, Long.TYPE),
                row.get("data_int8", Byte.class),
                row.get(2, Short.class),
                row.get(3, Short.class),
                row.get("data_uint16", Integer.class),
                row.get(5, Integer.class),
                row.get(6, Integer.class),
                row.get(7, Integer.class),
                row.get(8, Long.class),
                row.get("data_int64", Long.class),
                (BigInteger) row.get(10, Number.class) // For check class
            )));
        }

        private static Statement createSimpleInsert(Connection connection, IntsEntity... entities) {
            String[] values = new String[entities.length];

            for (int i = 0; i < values.length; ++i) {
                values[i] = entities[i].values();
            }

            return connection.createStatement(String.format("INSERT INTO test VALUES %s", String.join(",", values)))
                .returnGeneratedValues("generated_id");
        }

        private static Statement createPreparedInsert(Connection connection, IntsEntity... entities) {
            Statement statement = connection.createStatement("INSERT INTO test VALUES(DEFAULT,?,?,?,?,?,?,?,?,?,?)");

            for (IntsEntity entity : entities) {
                entity.bind(statement);
            }

            return statement.returnGeneratedValues("generated_id");
        }

        private static List<IntsEntity> selectOdd(boolean isOdd, IntsEntity... entities) {
            return Stream.of(entities).filter(entity -> (entity.id % 2 == 1) == isOdd).collect(Collectors.toList());
        }
    }
}
