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
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Base class considers data integration unit tests for implementations of {@link CompatibilityTestSupport}.
 */
abstract class IntegrationTestSupport extends CompatibilityTestSupport {

    private static final short UINT8_MAX_VALUE = 255;

    private static final int UINT16_MAX_VALUE = 65535;

    private static final int INT24_MIN_VALUE = -8388608;

    private static final int INT24_MAX_VALUE = 8388607;

    private static final int UINT24_MAX_VALUE = 16777215;

    private static final long UINT32_MAX_VALUE = 4294967295L;

    private static final BigInteger UINT64_MAX_VALUE = new BigInteger("18446744073709551615");

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

    IntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        super(configuration);
    }

    @SuppressWarnings({"varargs", "unchecked"})
    abstract <T> void testType(Class<T> type, String defined, T... values);

    @Test
    void tinyintSigned() {
        testType(Byte.class, "TINYINT", (byte) 1, (byte) -1, null, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    @Test
    void tinyintUnsigned() {
        testType(Short.class, "TINYINT UNSIGNED", (short) 1, null, (short) 0, UINT8_MAX_VALUE);
    }

    @Test
    void smallintSigned() {
        testType(Short.class, "SMALLINT", (short) 1, (short) -1, null, Short.MIN_VALUE, Short.MAX_VALUE);
    }

    @Test
    void smallintUnsigned() {
        testType(Integer.class, "SMALLINT UNSIGNED", 1, null, 0, UINT16_MAX_VALUE);
    }

    @Test
    void mediumintSigned() {
        testType(Integer.class, "MEDIUMINT", 1, -1, null, INT24_MIN_VALUE, INT24_MAX_VALUE);
    }

    @Test
    void mediumintUnsigned() {
        testType(Integer.class, "MEDIUMINT UNSIGNED", 1, null, 0, UINT24_MAX_VALUE);
    }

    @Test
    void intSigned() {
        testType(Integer.class, "INT", 1, -1, null, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    @Test
    void intUnsigned() {
        testType(Long.class, "INT UNSIGNED", 1L, null, 0L, UINT32_MAX_VALUE);
    }

    @Test
    void bigintSigned() {
        testType(Long.class, "BIGINT", 1L, -1L, null, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    void bigintUnsigned() {
        testType(BigInteger.class, "BIGINT UNSIGNED", BigInteger.ONE, null, BigInteger.ZERO, UINT64_MAX_VALUE);
    }

    @Test
    void year() {
        testType(Year.class, "YEAR", null, Year.of(1901), Year.of(2155));
        testType(Short.class, "YEAR", null, (short) 1901, (short) 2155);
    }

    abstract void varchar();

    abstract void date();

    abstract void time();

    abstract void dateTime();

    abstract void timestamp();

    static Flux<Integer> extractId(Result result) {
        return Flux.from(result.map((row, metadata) -> row.get(0, Integer.class)));
    }

    static <T> Flux<Optional<T>> extractOptionalField(Result result, Class<T> type) {
        return Flux.from(result.map((row, metadata) -> Optional.ofNullable(row.get(0, type))));
    }
}
