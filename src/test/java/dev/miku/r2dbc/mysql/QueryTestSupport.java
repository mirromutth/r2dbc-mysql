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

import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Base class considers unit tests for implementations of {@link Query}.
 */
abstract class QueryTestSupport {

    @Test
    abstract void parse();

    @Test
    abstract void getIndexes();

    @Test
    abstract void bind();

    @Test
    abstract void rejectGetIndexes();

    @Test
    abstract void selfHashCode();

    @Test
    abstract void selfEquals();

    @Test
    abstract void indexesEquals();

    static void assertParametrizedQuery(String sql, String parsedSql, Map<String, int[]> nameKeyedIndexes, int parameters) {
        Query query = Query.parse(sql, true);

        assertThat(query).isExactlyInstanceOf(PrepareQuery.class)
            .extracting(PrepareQuery.class::cast)
            .satisfies(it -> {
                assertEquals(it.getSql(), parsedSql);
                assertEquals(it.getParameters(), parameters);
                assertEquals(it.getParameterNames(), nameKeyedIndexes.keySet());

                for (Map.Entry<String, int[]> entry : nameKeyedIndexes.entrySet()) {
                    ParameterIndex indexes = it.getIndexes(entry.getKey());
                    int[] right = entry.getValue();
                    int[] left = indexes.toIntArray();

                    assertArrayEquals(left, right, () -> String.format("expected: %s but was: %s", indexes, Arrays.toString(right)));
                }
            });

        query = Query.parse(sql, false);

        assertThat(query).isExactlyInstanceOf(TextQuery.class)
            .extracting(TextQuery.class::cast)
            .satisfies(it -> {
                assertEquals(String.join("?", it.getSqlParts()), parsedSql);
                assertEquals(it.getParameters(), parameters);
                assertEquals(it.getParameterNames(), nameKeyedIndexes.keySet());

                for (Map.Entry<String, int[]> entry : nameKeyedIndexes.entrySet()) {
                    ParameterIndex indexes = it.getIndexes(entry.getKey());
                    int[] right = entry.getValue();
                    int[] left = indexes.toIntArray();

                    assertArrayEquals(left, right, () -> String.format("expected: %s but was: %s", indexes, Arrays.toString(right)));
                }
            });
    }

    static Tuple2<String, int[]> link(String name, int... indexes) {
        if (indexes.length == 0) {
            throw new IllegalArgumentException("must has least one index");
        }

        return Tuples.of(name, indexes);
    }

    @SafeVarargs
    static Map<String, int[]> mapOf(Tuple2<String, int[]>... tuples) {
        // ceil(size / 0.75) = ceil((size * 4) / 3) = floor((size * 4 + 3 - 1) / 3)
        Map<String, int[]> result = new HashMap<>(((tuples.length << 2) + 2) / 3, 0.75f);

        for (Tuple2<String, int[]> tuple : tuples) {
            result.put(tuple.getT1(), tuple.getT2());
        }

        return result;
    }

    static final class MockValue implements ParameterValue {

        static final MockValue INSTANCE = new MockValue();

        private MockValue() {
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.empty();
        }

        @Override
        public short getType() {
            return DataTypes.INT;
        }

        @Override
        public void dispose() {
        }

        @Override
        public String toString() {
            return "MockValue{}";
        }
    }
}
