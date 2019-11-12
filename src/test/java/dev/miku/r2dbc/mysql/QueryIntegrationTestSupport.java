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
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Base class considers integration unit tests for queries and data.
 */
abstract class QueryIntegrationTestSupport extends IntegrationTestSupport {

    private static final short UINT8_MAX_VALUE = 255;

    private static final int UINT16_MAX_VALUE = 65535;

    private static final int INT24_MIN_VALUE = -8388608;

    private static final int INT24_MAX_VALUE = 8388607;

    private static final int UINT24_MAX_VALUE = 16777215;

    private static final long UINT32_MAX_VALUE = 4294967295L;

    private static final BigInteger UINT64_MAX_VALUE = new BigInteger("18446744073709551615");

    private static final List<Tuple2<Duration, LocalTime>> DURATION_TIME_CASES;

    static final Duration MIN_DURATION = Duration.ofSeconds(-TimeUnit.HOURS.toSeconds(838) - TimeUnit.MINUTES.toSeconds(59) - 59);

    static final Duration MAX_DURATION = Duration.ofSeconds(TimeUnit.HOURS.toSeconds(838) + TimeUnit.MINUTES.toSeconds(59) + 59);

    static final LocalTime MIN_TIME = LocalTime.MIDNIGHT;

    static final LocalTime MAX_TIME = LocalTime.of(23, 59, 59);

    static final LocalDate MIN_DATE = LocalDate.of(1000, 1, 1);

    static final LocalDate MAX_DATE = LocalDate.of(9999, 12, 31);

    static final LocalDateTime MIN_DATE_TIME = LocalDateTime.of(1000, 1, 1, 0, 0, 0);

    static final LocalDateTime MAX_DATE_TIME = LocalDateTime.of(9999, 12, 31, 23, 59, 59);

    static final LocalDateTime MIN_TIMESTAMP = LocalDateTime.of(1970, 1, 3, 0, 0, 0);

    static final LocalDateTime MAX_TIMESTAMP = LocalDateTime.of(2038, 1, 15, 23, 59, 59);

    static {
        Duration negativeOne = Duration.ofSeconds(-1);
        Duration negativeOneDayOneSecond = Duration.ofSeconds(-TimeUnit.DAYS.toSeconds(1) - 1);
        Duration oneDayOneSecond = Duration.ofSeconds(TimeUnit.DAYS.toSeconds(1) + 1);
        LocalTime lastSecond = LocalTime.of(23, 59, 59);
        LocalTime firstSecond = LocalTime.of(0, 0, 1);

        DURATION_TIME_CASES = Arrays.asList(
            Tuples.of(negativeOne, lastSecond),
            Tuples.of(negativeOneDayOneSecond, lastSecond),
            Tuples.of(oneDayOneSecond, firstSecond)
        );
    }

    QueryIntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        super(configuration);
    }

    @SuppressWarnings({"varargs", "unchecked"})
    abstract <T> void testType(Class<T> type, String defined, boolean quota, T... values);

    @Test
    void tinyintSigned() {
        testType(Byte.class, "TINYINT", false, (byte) 1, (byte) -1, null, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    @Test
    void tinyintUnsigned() {
        testType(Short.class, "TINYINT UNSIGNED", false, (short) 1, null, (short) 0, UINT8_MAX_VALUE);
    }

    @Test
    void smallintSigned() {
        testType(Short.class, "SMALLINT", false, (short) 1, (short) -1, null, Short.MIN_VALUE, Short.MAX_VALUE);
    }

    @Test
    void smallintUnsigned() {
        testType(Integer.class, "SMALLINT UNSIGNED", false, 1, null, 0, UINT16_MAX_VALUE);
    }

    @Test
    void mediumintSigned() {
        testType(Integer.class, "MEDIUMINT", false, 1, -1, null, INT24_MIN_VALUE, INT24_MAX_VALUE);
    }

    @Test
    void mediumintUnsigned() {
        testType(Integer.class, "MEDIUMINT UNSIGNED", false, 1, null, 0, UINT24_MAX_VALUE);
    }

    @Test
    void intSigned() {
        testType(Integer.class, "INT", false, 1, -1, null, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    @Test
    void intUnsigned() {
        testType(Long.class, "INT UNSIGNED", false, 1L, null, 0L, UINT32_MAX_VALUE);
    }

    @Test
    void bigintSigned() {
        testType(Long.class, "BIGINT", false, 1L, -1L, null, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    void bigintUnsigned() {
        testType(BigInteger.class, "BIGINT UNSIGNED", false, BigInteger.ONE, null, BigInteger.ZERO, UINT64_MAX_VALUE);
    }

    @Test
    void floatSigned() {
        testType(Float.class, "FLOAT", true, null, 1.0f, -1.0f);
    }

    @Test
    void floatUnsigned() {
        testType(Float.class, "FLOAT UNSIGNED", true, null, 1.0f);
    }

    @Test
    void doubleSigned() {
        testType(Double.class, "DOUBLE", true, null, 1.0, -1.0, Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE);
    }

    @Test
    void doubleUnsigned() {
        testType(Double.class, "DOUBLE UNSIGNED", true, null, 1.0, Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE);
    }

    @Test
    void decimalSigned() {
        testType(BigDecimal.class, "DECIMAL(10,2)", true, null, new BigDecimal("1.00"), new BigDecimal("-1.00"), new BigDecimal("1.99"));
    }

    @Test
    void decimalUnsigned() {
        testType(BigDecimal.class, "DECIMAL(10,2) UNSIGNED", true, null, new BigDecimal("1.00"), new BigDecimal("1.99"));
    }

    @Test
    void year() {
        testType(Year.class, "YEAR", false, null, Year.of(1901), Year.of(2155));
        testType(Short.class, "YEAR", false, null, (short) 1901, (short) 2155);
    }

    @Test
    void varchar() {
        testType(String.class, "VARCHAR(50)", true, "", null, "data");
        testType(String.class, "CHAR(50)", true, "", null, "data");
    }

    @Test
    void enumerable() {
        testType(String.class, "ENUM('ONE','TWO','THREE')", true, null, "ONE", "TWO", "THREE");
        testType(EnumData.class, "ENUM('ONE','TWO','THREE')", true, null, EnumData.ONE, EnumData.TWO, EnumData.THREE);
    }

    /**
     * See https://github.com/mirromutth/r2dbc-mysql/issues/69 .
     */
    @Test
    abstract void consumePortion();

    /**
     * https://github.com/mirromutth/r2dbc-mysql/issues/73 .
     */
    @Test
    abstract void ignoreResult();

    /**
     * See https://github.com/mirromutth/r2dbc-mysql/issues/62 .
     */
    @Test
    abstract void varbinary();

    @Test
    abstract void bit();

    @Test
    abstract void set();

    @Test
    void json() {
        // MySQL 5.5 and 5.6 unsupported JSON, just succeed.
    }

    @Test
    abstract void date();

    @Test
    abstract void time();

    @Test
    void timeDuration() {
        connectionFactory.create()
            .flatMap(connection -> {
                // Should use simple statement for table definition.
                return Mono.from(connection.createStatement("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value TIME)")
                    .execute())
                    .flatMap(IntegrationTestSupport::extractRowsUpdated)
                    .thenMany(Flux.fromIterable(DURATION_TIME_CASES).concatMap(pair -> testTimeDuration(connection, pair.getT1(), pair.getT2())))
                    .concatWith(close(connection))
                    .then();
            })
            .as(StepVerifier::create)
            .verifyComplete();
    }

    abstract Mono<Void> testTimeDuration(Connection connection, Duration origin, LocalTime time);

    @Test
    abstract void dateTime();

    @Test
    abstract void timestamp();

    static Flux<Integer> extractId(Result result) {
        return Flux.from(result.map((row, metadata) -> row.get(0, Integer.class)));
    }

    @SuppressWarnings("unchecked")
    static <T> Flux<Optional<T>> extractOptionalField(Result result, Type type) {
        if (type instanceof Class<?>) {
            return Flux.from(result.map((row, metadata) -> Optional.ofNullable(row.get(0, (Class<T>) type))));
        }
        return Flux.from(result.map((row, metadata) -> Optional.ofNullable(((MySqlRow) row).get(0, (ParameterizedType) type))));
    }

    enum EnumData {

        ONE,
        TWO,
        THREE
    }
}
