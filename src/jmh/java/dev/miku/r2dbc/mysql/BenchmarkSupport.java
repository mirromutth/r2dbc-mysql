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

import io.r2dbc.spi.Connection;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Base class considers global benchmark settings.
 */
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 10, time = 1)
@Fork(value = 5, jvmArgsAppend = {
    "-server", "-Xms4g", "-Xmx4g", "-Xmn1536m", "-XX:CMSInitiatingOccupancyFraction=82", "-Xss256k",
    "-XX:+DisableExplicitGC", "-XX:+UseConcMarkSweepGC", "-XX:+UseParNewGC", "-XX:+CMSParallelRemarkEnabled",
    "-XX:ParallelGCThreads=8", "-XX:LargePageSizeInBytes=128m", "-XX:+UseFastAccessorMethods",
    "-XX:+CMSScavengeBeforeRemark", "-XX:CMSInitiatingOccupancyFraction=90",
    "-XX:+UseCMSInitiatingOccupancyOnly", "-XX:+CMSClassUnloadingEnabled"
})
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public abstract class BenchmarkSupport {

    @State(Scope.Thread)
    public static class ConnectionState {

        private static final String PASSWORD;

        static {
            PASSWORD = System.getProperty("test.mysql.password");

            if (PASSWORD == null || PASSWORD.isEmpty()) {
                throw new IllegalStateException("Property test.mysql.password must exists and not be empty");
            }
        }

        protected Connection connection;

        @Setup(Level.Trial)
        public void doSetup() {
            MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
                .host("127.0.0.1")
                .connectTimeout(Duration.ofSeconds(3))
                .user("root")
                .password(PASSWORD)
                .database("r2dbc")
                .autodetectExtensions(false)
                .build();

            connection = MySqlConnectionFactory.from(configuration).create().block();
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            Mono.from(connection.close()).block();
        }
    }
}
