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

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.test.Example;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Extra unit tests for {@link Batch} which append to TCK.
 */
interface MySqlExampleBatchExtra extends MySqlExampleExtra {

    @Test
    default void batchCrud() {
        getJdbcOperations().execute(String.format("DROP TABLE `%s`", "test"));
        getJdbcOperations().execute(String.format("CREATE TABLE `%s` (`id` INT PRIMARY KEY AUTO_INCREMENT, `data` VARCHAR(20))", "test"));

        String isEven = "`id` % 2 = 0";
        String isOdd = "`id` % 2 = 1";
        String firstData = "first-data";
        String secondData = "second-data";
        String thirdData = "third-data";
        String fourthData = "fourth-data";
        String fifthData = "fifth-data";
        String sixthData = "sixth-data";
        String seventhData = "seventh-data";

        Mono.from(getConnectionFactory().create())
            .flatMap(connection -> {
                Batch selectBatch = connection.createBatch();
                Batch insertBatch = connection.createBatch();
                Batch updateBatch = connection.createBatch();
                Batch deleteBatch = connection.createBatch();
                Statement selectStmt = connection.createStatement(BatchHelper.formattedSelect(""));

                selectBatch.add(BatchHelper.formattedSelect(isEven));
                selectBatch.add(BatchHelper.formattedSelect(isOdd));

                insertBatch.add(BatchHelper.formattedInsert(firstData));
                insertBatch.add(BatchHelper.formattedInsert(secondData));
                insertBatch.add(BatchHelper.formattedInsert(thirdData));
                insertBatch.add(BatchHelper.formattedInsert(fourthData));
                insertBatch.add(BatchHelper.formattedInsert(fifthData));

                updateBatch.add(BatchHelper.formattedUpdate(sixthData, isEven));
                updateBatch.add(BatchHelper.formattedUpdate(seventhData, isOdd));

                deleteBatch.add(BatchHelper.formattedDelete(isOdd));
                deleteBatch.add(BatchHelper.formattedDelete(isEven));

                return Flux.from(insertBatch.execute())
                    .concatMap(r -> Mono.from(r.getRowsUpdated()))
                    .doOnNext(updated -> assertEquals(updated.intValue(), 1))
                    .reduce(Math::addExact)
                    .doOnNext(all -> assertEquals(all.intValue(), 5))
                    .then(Mono.from(selectStmt.execute()))
                    .flatMapMany(result -> result.map((row, metadata) -> row.get("data", String.class)))
                    .collectList()
                    .doOnNext(data -> assertEquals(data.size(), 5))
                    .doOnNext(data -> assertEquals(data, Arrays.asList(firstData, secondData, thirdData, fourthData, fifthData)))
                    .thenMany(updateBatch.execute())
                    .concatMap(r -> Mono.from(r.getRowsUpdated()))
                    .collectList()
                    .doOnNext(updated -> assertEquals(updated.size(), 2))
                    .doOnNext(updated -> assertEquals(updated, Arrays.asList(2, 3)))
                    .thenMany(selectBatch.execute())
                    .concatMap(result -> result.map((row, metadata) -> row.get("data", String.class)))
                    .collectList()
                    .doOnNext(data -> assertEquals(data.size(), 5))
                    .doOnNext(data -> assertEquals(data, Arrays.asList(sixthData, sixthData, seventhData, seventhData, seventhData)))
                    .thenMany(deleteBatch.execute())
                    .concatMap(r -> Mono.from(r.getRowsUpdated()))
                    .collectList()
                    .doOnNext(deleted -> assertEquals(deleted.size(), 2))
                    .doOnNext(deleted -> assertEquals(deleted, Arrays.asList(3, 2)))
                    .concatWith(Example.close(connection))
                    .then();
            })
            .as(StepVerifier::create)
            .verifyComplete();
    }

    final class BatchHelper {

        private static String formattedSelect(String condition) {
            if (condition.isEmpty()) {
                return "SELECT `id`, `data` FROM `test` ORDER BY `id`;\n";
            }
            return String.format("SELECT `id`, `data` FROM `test` WHERE %s ORDER BY `id`;\n", condition);
        }

        private static String formattedInsert(String data) {
            return String.format("INSERT INTO `test` (`data`) VALUES ('%s')", data);
        }

        private static String formattedUpdate(String data, String condition) {
            return String.format("UPDATE `test` SET `data` = '%s' WHERE %s", data, condition);
        }

        private static String formattedDelete(String condition) {
            return String.format("DELETE FROM `test` WHERE %s", condition);
        }

        private BatchHelper() {
        }
    }
}
