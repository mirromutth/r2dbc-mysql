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

import com.mysql.cj.MysqlConnection;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for {@link PrepareQuery}, {@link PrepareParametrizedStatement} and {@link PrepareSimpleStatement}.
 */
class PrepareQueryIntegrationTest extends QueryIntegrationTestSupport {

    PrepareQueryIntegrationTest() {
        super(configuration(sql -> true));
    }

    @Test
    void notSupportCompound() {
        String tdl = "CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value INT)";
        badGrammar(connection -> Mono.from(connection.createStatement(tdl).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("INSERT INTO test(`value`) VALUES (?); INSERT INTO test(`value`) VALUES (?)")
                .bind(0, 1)
                .bind(1, 2)
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated));

        Function<? super MySqlConnection, Flux<MySqlResult>> runner= connection ->  (connection.createStatement(tdl).execute());
    }

    @Test
    void fetchSize() {
        complete(connection -> Mono.from(connection.createStatement("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value INT)")
            .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then(Mono.from(connection.createStatement("INSERT INTO test(`value`) VALUES (1),(2),(3),(4),(5)").execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .add()
                .bind(0, 3)
                .add()
                .bind(0, 0)
                .fetchSize(2)
                .execute())
            .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.TYPE)))
            .collectList()
            .doOnNext(it -> assertEquals(it, Arrays.asList(1, 2, 3, 4, 5, 4, 5, 1, 2, 3, 4, 5))));
    }

    @Test
    void insertFetch() {
        complete(connection -> Mono.from(connection.createStatement("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value INT)")
            .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("INSERT INTO test(`value`) VALUES (?),(?),(?),(?),(?)")
                .bind(0, 1)
                .bind(1, 2)
                .bind(2, 3)
                .bind(3, 4)
                .bind(4, 5)
                .fetchSize(2)
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .reduce(Math::addExact)
            .doOnNext(it -> assertEquals(5, it)));
    }
}
