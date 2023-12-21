/*
 * Copyright 2018-2021 the original author or authors.
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

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

/**
 * Base class considers connection factory and general function for integration tests.
 */

abstract class IntegrationTestSupport {

    static MySQLContainer<?> mySQLContainer;
    private final MySqlConnectionFactory connectionFactory;

    IntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        this.connectionFactory = MySqlConnectionFactory.from(configuration);
    }

    void complete(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyComplete();
    }

    void badGrammar(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyError(R2dbcBadGrammarException.class);
    }

    Mono<MySqlConnection> create() {
        return connectionFactory.create();
    }

    private StepVerifier.FirstStep<Void> process(Function<? super MySqlConnection, Publisher<?>> runner) {
        return create()
            .flatMap(connection -> Flux.from(runner.apply(connection))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty()))
                .then())
            .as(StepVerifier::create);
    }

    static Mono<Long> extractRowsUpdated(Result result) {
        return Mono.from(result.getRowsUpdated());
    }

    static MySqlConnectionConfiguration configuration(boolean autodetectExtensions,
        @Nullable ZoneId serverZoneId, @Nullable Predicate<String> preferPrepared) {
        String user = "root";
        String password = System.getProperty("test.mysql.password");
        String databaseName = "r2dbc";
        String host = "127.0.0.1";
        int port = MYSQL_PORT;

        // it seems this is used in the CI for now
        // otherwise we could use an annotation based approach
        if (password == null && mySQLContainer == null) {
            mySQLContainer= new MySQLContainer<>(DockerImageName.parse("mysql").withTag("5.7.34"))
                    .withDatabaseName(databaseName)
                    .withUsername(user);
            mySQLContainer.start();
            Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .until(mySQLContainer::isRunning);
        }
        if (mySQLContainer !=null) {
            password = mySQLContainer.getPassword();
            host = mySQLContainer.getHost();
            port = mySQLContainer.getMappedPort(MYSQL_PORT);
        }

        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder()
            .host(host)
            .port(port)
            .connectTimeout(Duration.ofSeconds(3))
            .user(user)
            .password(password)
            .database(databaseName)
            .autodetectExtensions(autodetectExtensions);

        if (serverZoneId != null) {
            builder.serverZoneId(serverZoneId);
        }

        if (preferPrepared == null) {
            builder.useClientPrepareStatement();
        } else {
            builder.useServerPrepareStatement(preferPrepared);
        }

        return builder.build();
    }
}
