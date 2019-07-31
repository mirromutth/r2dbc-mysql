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

import io.github.mirromutth.r2dbc.mysql.constant.SslMode;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Simple to execute a task in the newly connection, any exceptions in the
 * execution will be thrown, connection will always be closed after use.
 */
final class MySqlConnectionRunner {

    private static final MySqlConnectionRunner[] ALL_RUNNER = {
        new MySqlConnectionRunner(MySql55Example.CONFIGURATION),
        new MySqlConnectionRunner(MySql56Example.CONFIGURATION),
        new MySqlConnectionRunner(MySql57Example.CONFIGURATION),
        new MySqlConnectionRunner(MySql80Example.CONFIGURATION)
    };

    private final MySqlConnectionConfiguration configuration;

    private MySqlConnectionRunner(MySqlConnectionConfiguration configuration) {
        this.configuration = configuration;
    }

    private void complete(Function<MySqlConnection, Publisher<?>> f) {
        verifier(f, null).verifyComplete();
    }

    private void except(Class<? extends Throwable> e, Function<MySqlConnection, Publisher<?>> f, Consumer<Throwable> consumer) {
        verifier(f, consumer).verifyError(e);
    }

    private StepVerifier.FirstStep<Void> verifier(Function<MySqlConnection, Publisher<?>> f, @Nullable Consumer<Throwable> consumer) {
        return StepVerifier.create(MySqlConnectionFactory.from(configuration)
            .create()
            .flatMap(connection -> Flux.from(f.apply(connection))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty()))
                .then()
                .as(r -> {
                    if (consumer == null) {
                        return r;
                    } else {
                        return r.doOnError(consumer);
                    }
                })));
    }

    static void completeAll(Function<MySqlConnection, Publisher<?>> f) {
        for (MySqlConnectionRunner runner : ALL_RUNNER) {
            runner.complete(f);
        }
    }

    static void exceptAll(Class<? extends Throwable> e, Function<MySqlConnection, Publisher<?>> f, Consumer<Throwable> consumer) {
        for (MySqlConnectionRunner runner : ALL_RUNNER) {
            runner.except(e, f, consumer);
        }
    }
}
