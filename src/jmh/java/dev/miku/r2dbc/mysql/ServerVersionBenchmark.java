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

import dev.miku.r2dbc.mysql.util.ServerVersion;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmarks for {@link ServerVersion}.
 */
@State(Scope.Benchmark)
@Testable
public class ServerVersionBenchmark extends BenchmarkSupport {

    private static final String VERSION_STR = "8.0.12-some-web-services-1.21.41-community-openssl-1.1.1";

    private static final ServerVersion VERSION = ServerVersion.parse(VERSION_STR);

    @Benchmark
    @Testable
    public void parse() {
        ServerVersion.parse(VERSION_STR);
    }

    @Benchmark
    @Testable
    public boolean isEnterprise() {
        return VERSION.isEnterprise();
    }
}
