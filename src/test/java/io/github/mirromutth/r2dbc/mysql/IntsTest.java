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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.mirromutth.r2dbc.mysql.MySqlConnectionRunner.completeAll;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for CRUD integers.
 */
class IntsTest {

    private static final long FIRST_ID = 6L;

    /**
     * Note: should be an odd integer to control half of the ID is even half is odd integer.
     */
    private static final int INCREMENT_STEP = 5;

    /**
     * Note: MySQL does not support table definition in prepare statement.
     */
    private static final String TABLE = String.format("CREATE TEMPORARY TABLE `test_ints`\n" +
        "(\n" +
        "    `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n" +
        "    `data_int8`   TINYINT,\n" +
        "    `data_uint8`  TINYINT UNSIGNED,\n" +
        "    `data_int16`  SMALLINT,\n" +
        "    `data_uint16` SMALLINT UNSIGNED,\n" +
        "    `data_int24`  MEDIUMINT,\n" +
        "    `data_uint24` MEDIUMINT UNSIGNED,\n" +
        "    `data_int32`  INT,\n" +
        "    `data_uint32` INT UNSIGNED,\n" +
        "    `data_int64`  BIGINT,\n" +
        "    `data_uint64` BIGINT UNSIGNED\n" +
        ") AUTO_INCREMENT = %d;\n", FIRST_ID);

    private static final String INSERT_INTO = "INSERT INTO `test_ints`\n" +
        "(`data_int8`, `data_uint8`, `data_int16`, `data_uint16`, `data_int24`, `data_uint24`, `data_int32`, `data_uint32`, `data_int64`, `data_uint64`)\n" +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);\n";

    private static final Entity ALL_NULL = new Entity(FIRST_ID, null, null, null, null, null, null, null, null, null, null);

    private static final Entity ODD_HALF_NULL = new Entity(FIRST_ID + INCREMENT_STEP, null, (short) 11, null, 22, null, 33, null, 44L, null, BigInteger.valueOf(55));

    private static final Entity EVEN_HALF_NULL = new Entity(FIRST_ID + INCREMENT_STEP * 2, (byte) -11, null, (short) -22, null, -33, null, -44, null, -44L, null);

    private static final Entity FULL = new Entity(FIRST_ID + INCREMENT_STEP * 3, (byte) -66, (short) 77, (short) 88, 99, -1010, 1111, 1212, 1313L, -1414L, BigInteger.valueOf(1515));

    @Test
    void crudByParametrizedStatement() {
        completeAll(connection -> Mono.from(connection.createStatement("SET @@auto_increment_increment=?")
            .bind(0, (Integer) INCREMENT_STEP)
            .execute())
            .flatMap(MySqlResult::getRowsUpdated)
            .then(Mono.from(connection.createStatement(TABLE).execute()))
            .flatMap(MySqlResult::getRowsUpdated)
            .then(Mono.from(connection.createStatement("SELECT * FROM `test_ints`").execute()))
            .flatMapMany(result -> result.map((row, metadata) -> 1))
            .collectList()
            .doOnNext(entities -> assertTrue(entities.isEmpty()))
            .thenMany(createInsert(connection, ALL_NULL, ODD_HALF_NULL, EVEN_HALF_NULL, FULL).execute())
            .concatMap(result -> result.getRowsUpdated()
                .doOnNext(u -> assertEquals(u.intValue(), 1))
                .thenMany(result.map((row, metadata) -> row.get("generated_id", Long.TYPE))))
            .collectList()
            .doOnNext(ids -> assertEquals(ids, Arrays.asList(FIRST_ID, FIRST_ID + INCREMENT_STEP, FIRST_ID + INCREMENT_STEP * 2, FIRST_ID + INCREMENT_STEP * 3)))
            .thenMany(connection.createStatement("SELECT * FROM `test_ints` WHERE `id` % 2 = ?is_odd")
                .bind("is_odd", true)
                .add()
                .bind("is_odd", false)
                .execute())
            .concatMap(result -> Entity.from(result).collectList())
            .collectList()
            .doOnNext(entities -> assertEquals(entities, Arrays.asList(selectEntities(true), selectEntities(false))))
            .thenMany(connection.createStatement("DELETE FROM `test_ints` WHERE `id` % 2 = ?is_odd")
                .bind("is_odd", true)
                .add()
                .bind("is_odd", false)
                .execute())
            .concatMap(MySqlResult::getRowsUpdated)
            .collectList()
            .doOnNext(rowsUpdated -> assertEquals(rowsUpdated, Arrays.asList(2, 2)))
        );
    }

    private static List<Entity> selectEntities(boolean isOdd) {
        return Stream.of(ALL_NULL, ODD_HALF_NULL, EVEN_HALF_NULL, FULL).filter(entity -> (entity.id % 2 == 1) == isOdd).collect(Collectors.toList());
    }

    private static MySqlStatement createInsert(MySqlConnection connection, Entity... entities) {
        MySqlStatement statement = connection.createStatement(INSERT_INTO);

        for (Entity entity : entities) {
            bindInsertEntity(statement, entity).add();
        }

        statement.returnGeneratedValues("generated_id");

        return statement;
    }

    private static MySqlStatement bindInsertEntity(MySqlStatement statement, Entity entity) {
        if (entity.int8 != null) {
            statement.bind(0, entity.int8);
        } else {
            statement.bindNull(0, Byte.class);
        }

        if (entity.uint8 != null) {
            statement.bind(1, entity.uint8);
        } else {
            statement.bindNull(1, Short.class);
        }

        if (entity.int16 != null) {
            statement.bind(2, entity.int16);
        } else {
            statement.bindNull(2, Short.class);
        }

        if (entity.uint16 != null) {
            statement.bind(3, entity.uint16);
        } else {
            statement.bindNull(3, Integer.class);
        }

        if (entity.int24 != null) {
            statement.bind(4, entity.int24);
        } else {
            statement.bindNull(4, Integer.class);
        }

        if (entity.uint24 != null) {
            statement.bind(5, entity.uint24);
        } else {
            statement.bindNull(5, Integer.class);
        }

        if (entity.int32 != null) {
            statement.bind(6, entity.int32);
        } else {
            statement.bindNull(6, Integer.class);
        }

        if (entity.uint32 != null) {
            statement.bind(7, entity.uint32);
        } else {
            statement.bindNull(7, Long.class);
        }

        if (entity.int64 != null) {
            statement.bind(8, entity.int64);
        } else {
            statement.bindNull(8, Long.class);
        }

        if (entity.uint64 != null) {
            statement.bind(9, entity.uint64);
        } else {
            statement.bindNull(9, BigInteger.class);
        }

        return statement;
    }

    private static final class Entity {

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

        private Entity(
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
            if (!(o instanceof Entity)) {
                return false;
            }

            Entity entity = (Entity) o;

            return id == entity.id &&
                Objects.equals(int8, entity.int8) &&
                Objects.equals(uint8, entity.uint8) &&
                Objects.equals(int16, entity.int16) &&
                Objects.equals(uint16, entity.uint16) &&
                Objects.equals(int24, entity.int24) &&
                Objects.equals(uint24, entity.uint24) &&
                Objects.equals(int32, entity.int32) &&
                Objects.equals(uint32, entity.uint32) &&
                Objects.equals(int64, entity.int64) &&
                Objects.equals(uint64, entity.uint64);
        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + (int8 != null ? int8.hashCode() : 0);
            result = 31 * result + (uint8 != null ? uint8.hashCode() : 0);
            result = 31 * result + (int16 != null ? int16.hashCode() : 0);
            result = 31 * result + (uint16 != null ? uint16.hashCode() : 0);
            result = 31 * result + (int24 != null ? int24.hashCode() : 0);
            result = 31 * result + (uint24 != null ? uint24.hashCode() : 0);
            result = 31 * result + (int32 != null ? int32.hashCode() : 0);
            result = 31 * result + (uint32 != null ? uint32.hashCode() : 0);
            result = 31 * result + (int64 != null ? int64.hashCode() : 0);
            return 31 * result + (uint64 != null ? uint64.hashCode() : 0);
        }

        @Override
        public String toString() {
            return "Entity{" +
                "id=" + id +
                ", int8=" + int8 +
                ", uint8=" + uint8 +
                ", int16=" + int16 +
                ", uint16=" + uint16 +
                ", int24=" + int24 +
                ", uint24=" + uint24 +
                ", int32=" + int32 +
                ", uint32=" + uint32 +
                ", int64=" + int64 +
                ", uint64=" + uint64 +
                '}';
        }

        private static Flux<Entity> from(MySqlResult result) {
            return Flux.from(result.map((row, metadata) -> new Entity(
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
    }
}
