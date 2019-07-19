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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Simple to execute a task in the newly connection, any exceptions in the
 * execution will be thrown, connection will always be closed after use.
 */
@FunctionalInterface
public interface MySqlConnectionRunner {

    /**
     * MySQL 5.7.x community version with SSL.
     */
    MySqlConnectionRunner SSL_COMMUNITY_5_7 = MySqlConnectionRunner.ofVersion("5_7");

    void run(Function<MySqlConnection, Publisher<?>> consumer) throws Throwable;

    static MySqlConnectionRunner ofVersion(String version) {
        return (consumer) -> {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Throwable> cause = new AtomicReference<>();

            MySQLHelper.getFactoryByVersion(version).create()
                .flatMap(connection -> Flux.from(consumer.apply(connection))
                    .onErrorResume(e -> connection.close().then(Mono.error(e)))
                    .concatWith(connection.close().then(Mono.empty()))
                    .then())
                .subscribe(null, e -> {
                    cause.set(e);
                    latch.countDown();
                }, latch::countDown);

            latch.await();

            Throwable e = cause.get();
            if (e != null) {
                throw e;
            }
        };
    }
}
