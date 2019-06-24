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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;

import static io.github.mirromutth.r2dbc.mysql.MySqlConnectionRunner.STD5_7;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link MySqlBatch}.
 */
class MySqlBatchTest {

    private static final String TABLE_DDL = "CREATE TEMPORARY TABLE `batch` (\n" +
        "    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n" +
        "    `data` VARCHAR(20) NOT NULL\n" +
        ");\n";

    private static final String IS_EVEN = "`id` % 2 = 0";

    private static final String IS_ODD = "`id` % 2 = 1";

    private final String firstData = "first-data";

    private final String secondData = "second-data";

    private final String thirdData = "third-data";

    private final String fourthData = "fourth-data";

    private final String fifthData = "fifth-data";

    private final String sixthData = "sixth-data";

    private final String seventhData = "seventh-data";

    @Test
    void crudAllInOne() throws Throwable {
        STD5_7.run(Duration.ofSeconds(10), connection -> {
            MySqlBatch selectBatch = connection.createBatch();
            MySqlBatch insertBatch = connection.createBatch();
            MySqlBatch updateBatch = connection.createBatch();
            MySqlBatch deleteBatch = connection.createBatch();
            MySqlStatement selectStmt = connection.createStatement(formattedSelect(""));

            selectBatch.add(formattedSelect(IS_EVEN));
            selectBatch.add(formattedSelect(IS_ODD));

            insertBatch.add(formattedInsert(firstData));
            insertBatch.add(formattedInsert(secondData));
            insertBatch.add(formattedInsert(thirdData));
            insertBatch.add(formattedInsert(fourthData));
            insertBatch.add(formattedInsert(fifthData));

            updateBatch.add(formattedUpdate(sixthData, IS_EVEN));
            updateBatch.add(formattedUpdate(seventhData, IS_ODD));

            deleteBatch.add(formattedDelete(IS_ODD));
            deleteBatch.add(formattedDelete(IS_EVEN));

            return Mono.from(connection.createStatement(TABLE_DDL).execute())
                .flatMap(MySqlResult::getRowsUpdated)
                .thenMany(insertBatch.execute())
                .concatMap(MySqlResult::getRowsUpdated)
                .doOnNext(updated -> assertEquals(updated.intValue(), 1))
                .reduce(Math::addExact)
                .doOnNext(all -> assertEquals(all.intValue(), 5))
                .then(Mono.from(selectStmt.execute()))
                .flatMapMany(result -> result.map((row, metadata) -> row.get("data", String.class)))
                .collectList()
                .doOnNext(data -> assertEquals(data.size(), 5))
                .doOnNext(data -> assertEquals(data, Arrays.asList(firstData, secondData, thirdData, fourthData, fifthData)))
                .thenMany(updateBatch.execute())
                .concatMap(MySqlResult::getRowsUpdated)
                .collectList()
                .doOnNext(updated -> assertEquals(updated.size(), 2))
                .doOnNext(updated -> assertEquals(updated, Arrays.asList(2, 3)))
                .thenMany(selectBatch.execute())
                .concatMap(result -> result.map((row, metadata) -> row.get("data", String.class)))
                .collectList()
                .doOnNext(data -> assertEquals(data.size(), 5))
                .doOnNext(data -> assertEquals(data, Arrays.asList(sixthData, sixthData, seventhData, seventhData, seventhData)))
                .thenMany(deleteBatch.execute())
                .concatMap(MySqlResult::getRowsUpdated)
                .collectList()
                .doOnNext(deleted -> assertEquals(deleted.size(), 2))
                .doOnNext(deleted -> assertEquals(deleted, Arrays.asList(3, 2)));
        });
    }

    private static String formattedSelect(@Nullable String condition) {
        if (condition == null || condition.isEmpty()) {
            return "SELECT `id`, `data` FROM `batch` ORDER BY `id`;\n";
        }
        return String.format("SELECT `id`, `data` FROM `batch` WHERE %s ORDER BY `id`;\n", condition);
    }

    private static String formattedInsert(String data) {
        return String.format("INSERT INTO `batch` (`data`) VALUES ('%s')", data);
    }

    private static String formattedUpdate(String data, String condition) {
        return String.format("UPDATE `batch` SET `data` = '%s' WHERE %s", data, condition);
    }

    private static String formattedDelete(String condition) {
        return String.format("DELETE FROM `batch` WHERE %s", condition);
    }
}
