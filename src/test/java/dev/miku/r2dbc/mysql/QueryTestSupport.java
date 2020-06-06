/*
 * Copyright 2018-2020 the original author or authors.
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
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.HashMap;
import java.util.Map;

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

    static final class MockParameter implements Parameter {

        static final MockParameter INSTANCE = new MockParameter();

        private MockParameter() {
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public Mono<ByteBuf> binary() {
            return Mono.error(() -> new IllegalStateException("Mock parameter, has no value"));
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
            return Mono.error(() -> new IllegalStateException("Mock parameter, has no value"));
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
            return "MockParameter{}";
        }
    }
}
