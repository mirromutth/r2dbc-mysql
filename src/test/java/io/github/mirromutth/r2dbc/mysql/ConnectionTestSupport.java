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

import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link MySqlConnection}.
 */
abstract class ConnectionTestSupport extends CompatibilityTestSupport {

    ConnectionTestSupport(MySqlConnectionConfiguration configuration) {
        super(configuration);
    }

    @Test
    void selectFromOtherDatabase() {
        complete(connection -> Flux.from(connection.createStatement("SELECT * FROM `information_schema`.`innodb_trx`").execute())
            .flatMap(result -> result.map((row, metadata) -> row.get(0))));
    }

    @Test
    void isInTransaction() {
        complete(connection -> Mono.<Void>fromRunnable(() -> assertFalse(connection.isInTransaction()))
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertTrue(connection.isInTransaction()))
            .then(connection.commitTransaction())
            .doOnSuccess(ignored -> assertFalse(connection.isInTransaction()))
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertTrue(connection.isInTransaction()))
            .then(connection.rollbackTransaction())
            .doOnSuccess(ignored -> assertFalse(connection.isInTransaction())));
    }

    @Test
    void isAutoCommit() {
        complete(connection -> Mono.<Void>fromRunnable(() -> assertTrue(connection.isAutoCommit()))
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertFalse(connection.isAutoCommit()))
            .then(connection.commitTransaction())
            .doOnSuccess(ignored -> assertTrue(connection.isAutoCommit()))
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertFalse(connection.isAutoCommit()))
            .then(connection.rollbackTransaction())
            .doOnSuccess(ignored -> assertTrue(connection.isAutoCommit())));
    }

    @Test
    void commitTransactionWithoutBegin() {
        complete(MySqlConnection::commitTransaction);
    }

    @Test
    void rollbackTransactionWithoutBegin() {
        complete(MySqlConnection::rollbackTransaction);
    }

    @Test
    void rejectInvalidSavepoint() {
        complete(connection -> {
            assertThrows(IllegalArgumentException.class, () -> connection.createSavepoint(""));
            assertThrows(IllegalArgumentException.class, () -> connection.createSavepoint("`"));
            return Mono.empty();
        });
    }

    @Test
    void createSavepoint() {
        complete(connection -> connection.createSavepoint("foo"));
    }

    @Test
    void setTransactionIsolationLevel() {
        complete(connection -> connection.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED));
    }

    @Test
    void batchCrud() {
        String isEven = "id % 2 = 0";
        String isOdd = "id % 2 = 1";
        String firstData = "first-data";
        String secondData = "second-data";
        String thirdData = "third-data";
        String fourthData = "fourth-data";
        String fifthData = "fifth-data";
        String sixthData = "sixth-data";
        String seventhData = "seventh-data";

        complete(connection -> {
            MySqlBatch selectBatch = connection.createBatch();
            MySqlBatch insertBatch = connection.createBatch();
            MySqlBatch updateBatch = connection.createBatch();
            MySqlBatch deleteBatch = connection.createBatch();
            MySqlStatement selectStmt = connection.createStatement(formattedSelect(""));

            selectBatch.add(formattedSelect(isEven));
            selectBatch.add(formattedSelect(isOdd));

            insertBatch.add(formattedInsert(firstData));
            insertBatch.add(formattedInsert(secondData));
            insertBatch.add(formattedInsert(thirdData));
            insertBatch.add(formattedInsert(fourthData));
            insertBatch.add(formattedInsert(fifthData));

            updateBatch.add(formattedUpdate(sixthData, isEven));
            updateBatch.add(formattedUpdate(seventhData, isOdd));

            deleteBatch.add(formattedDelete(isOdd));
            deleteBatch.add(formattedDelete(isEven));

            return Mono.from(connection.createStatement("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value VARCHAR(20))")
                .execute())
                .thenMany(Flux.from(insertBatch.execute()))
                .concatMap(r -> Mono.from(r.getRowsUpdated()))
                .doOnNext(updated -> assertEquals(updated.intValue(), 1))
                .reduce(Math::addExact)
                .doOnNext(all -> assertEquals(all.intValue(), 5))
                .then(Mono.from(selectStmt.execute()))
                .flatMapMany(result -> result.map((row, metadata) -> row.get("value", String.class)))
                .collectList()
                .doOnNext(data -> assertEquals(data.size(), 5))
                .doOnNext(data -> assertEquals(data, Arrays.asList(firstData, secondData, thirdData, fourthData, fifthData)))
                .thenMany(updateBatch.execute())
                .concatMap(r -> Mono.from(r.getRowsUpdated()))
                .collectList()
                .doOnNext(updated -> assertEquals(updated.size(), 2))
                .doOnNext(updated -> assertEquals(updated, Arrays.asList(2, 3)))
                .thenMany(selectBatch.execute())
                .concatMap(result -> result.map((row, metadata) -> row.get("value", String.class)))
                .collectList()
                .doOnNext(data -> assertEquals(data.size(), 5))
                .doOnNext(data -> assertEquals(data, Arrays.asList(sixthData, sixthData, seventhData, seventhData, seventhData)))
                .thenMany(deleteBatch.execute())
                .concatMap(r -> Mono.from(r.getRowsUpdated()))
                .collectList()
                .doOnNext(deleted -> assertEquals(deleted.size(), 2))
                .doOnNext(deleted -> assertEquals(deleted, Arrays.asList(3, 2)))
                .then();
        });
    }

    private void complete(Function<? super MySqlConnection, Publisher<?>> runner) {
        run(runner).then().as(StepVerifier::create).verifyComplete();
    }

    private Mono<Void> run(Function<? super MySqlConnection, Publisher<?>> runner) {
        return connectionFactory.create()
            .flatMap(connection -> Flux.from(runner.apply(connection))
                .onErrorResume(e -> close(connection).then(Mono.error(e)))
                .concatWith(close(connection))
                .then());
    }

    private static String formattedSelect(String condition) {
        if (condition.isEmpty()) {
            return "SELECT id,value FROM test ORDER BY id";
        }
        return String.format("SELECT id,value FROM test WHERE %s ORDER BY id", condition);
    }

    private static String formattedInsert(String data) {
        return String.format("INSERT INTO test(value)VALUES('%s')", data);
    }

    private static String formattedUpdate(String data, String condition) {
        return String.format("UPDATE test SET value='%s' WHERE %s", data, condition);
    }

    private static String formattedDelete(String condition) {
        return String.format("DELETE FROM test WHERE %s", condition);
    }
}
