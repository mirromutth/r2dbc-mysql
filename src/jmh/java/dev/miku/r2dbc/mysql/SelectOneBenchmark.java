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

import io.r2dbc.spi.Statement;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import reactor.core.publisher.Flux;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for execute {@code SELECT 1}.
 */
@State(Scope.Benchmark)
@Threads(Threads.MAX)
@Timeout(time = 10)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Testable
public class SelectOneBenchmark extends BenchmarkSupport {

    @Benchmark
    @Testable
    public BigInteger selectOne(ConnectionState state) {
        Statement statement = state.connection.createStatement("SELECT 1");
        BigInteger val = Flux.from(statement.execute())
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, BigInteger.class)))
            .blockLast();
        if (val == null || 1 != val.intValue()) {
            throw new IllegalStateException("ERROR different to val:" + val);
        }
        return val;
    }
}
