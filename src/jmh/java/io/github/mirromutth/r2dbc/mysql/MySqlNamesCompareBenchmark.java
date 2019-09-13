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

import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

/**
 * Benchmark between {@link MySqlNames#compare} and {@link String#compareToIgnoreCase}.
 */
@State(Scope.Benchmark)
@Threads(1)
@Testable
public class MySqlNamesCompareBenchmark extends BenchmarkSupport {

    private static final String LEFT = "This is a test message for testing String compare with case insensitive or case sensitive";

    private static final String RIGHT = LEFT.toUpperCase();

    @Benchmark
    @Testable
    public int compareCi() {
        return MySqlNames.compare(LEFT, RIGHT);
    }

    @Benchmark
    @Testable
    public int nativeCompareCi() {
        return String.CASE_INSENSITIVE_ORDER.compare(LEFT, RIGHT);
    }

    @Benchmark
    @Testable
    public int compareCs() {
        return MySqlNames.compare(LEFT, LEFT);
    }

    @Benchmark
    @Testable
    public int nativeCompareCs() {
        return String.CASE_INSENSITIVE_ORDER.compare(LEFT, LEFT);
    }
}
