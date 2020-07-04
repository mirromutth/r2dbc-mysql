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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;

import static io.r2dbc.spi.IsolationLevel.READ_COMMITTED;
import static io.r2dbc.spi.IsolationLevel.READ_UNCOMMITTED;
import static io.r2dbc.spi.IsolationLevel.REPEATABLE_READ;
import static io.r2dbc.spi.IsolationLevel.SERIALIZABLE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link MySqlConnection}.
 */
class ConnectionIntegrationTest extends IntegrationTestSupport {

    ConnectionIntegrationTest() {
        super(configuration(false, null, null));
    }

    @Test
    void isInTransaction() {
        complete(connection -> Mono.<Void>fromRunnable(() -> assertThat(connection.isInTransaction()).isFalse())
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
            .then(connection.commitTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isFalse())
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
            .then(connection.rollbackTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isFalse()));
    }

    @Test
    void setAutoCommit() {
        complete(connection -> Mono.<Void>fromRunnable(() -> assertThat(connection.isAutoCommit()).isTrue())
            .then(connection.setAutoCommit(false))
            .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isFalse())
            .then(connection.setAutoCommit(true))
            .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isTrue()));
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
    void setTransactionIsolationLevel() {
        complete(connection -> Flux.just(READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE)
            .concatMap(level -> connection.setTransactionIsolationLevel(level)
                .doOnSuccess(ignored -> assertThat(level).isEqualTo(connection.getTransactionIsolationLevel()))));
    }

    @Test
    void batchCrud() {
        // TODO: spilt it to multiple test cases and move it to BatchIntegrationTest
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
                .thenMany(insertBatch.execute())
                .concatMap(r -> Mono.from(r.getRowsUpdated()))
                .doOnNext(updated -> assertThat(updated).isEqualTo(1))
                .reduce(Math::addExact)
                .doOnNext(all -> assertThat(all).isEqualTo(5))
                .then(Mono.from(selectStmt.execute()))
                .flatMapMany(result -> result.map((row, metadata) -> row.get("value", String.class)))
                .collectList()
                .doOnNext(data -> assertThat(data).isEqualTo(Arrays.asList(firstData, secondData, thirdData, fourthData, fifthData)))
                .thenMany(updateBatch.execute())
                .concatMap(r -> Mono.from(r.getRowsUpdated()))
                .collectList()
                .doOnNext(updated -> assertThat(updated).isEqualTo(Arrays.asList(2, 3)))
                .thenMany(selectBatch.execute())
                .concatMap(result -> result.map((row, metadata) -> row.get("value", String.class)))
                .collectList()
                .doOnNext(data -> assertThat(data).isEqualTo(Arrays.asList(sixthData, sixthData, seventhData, seventhData, seventhData)))
                .thenMany(deleteBatch.execute())
                .concatMap(r -> Mono.from(r.getRowsUpdated()))
                .collectList()
                .doOnNext(deleted -> assertThat(deleted).isEqualTo(Arrays.asList(3, 2)))
                .then();
        });
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
