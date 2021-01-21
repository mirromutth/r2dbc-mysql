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

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link MySqlBatchingBatch}.
 */
class MySqlBatchingBatchTest {

    private final MySqlBatchingBatch batch = new MySqlBatchingBatch(mock(Client.class), mock(Codecs.class),
        ConnectionContextTest.mock());

    @Test
    void add() {
        batch.add("");
        batch.add("INSERT INTO `test` VALUES (100)");
        batch.add("    ");
        batch.add("INSERT INTO `test` VALUES (100);");
        batch.add("INSERT INTO `test` VALUES (100);    ");
        batch.add("INSERT INTO `test` VALUES (100);    INSERT INTO `test` VALUES (100); " +
            "INSERT INTO `test` VALUES (100)   ");
        batch.add("");
        batch.add("   ;   INSERT INTO `test` VALUES (100);    INSERT INTO `test` VALUES (100); " +
            "INSERT INTO `test` VALUES (100);  ");

        assertEquals(batch.getSql(), ";INSERT INTO `test` VALUES (100);" +
            "    ;INSERT INTO `test` VALUES (100);" +
            "INSERT INTO `test` VALUES (100);" +
            "INSERT INTO `test` VALUES (100);    INSERT INTO `test` VALUES (100); " +
            "INSERT INTO `test` VALUES (100)   ;" +
            ";   ;   INSERT INTO `test` VALUES (100);    INSERT INTO `test` VALUES (100); " +
            "INSERT INTO `test` VALUES (100)");
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badAdd() {
        assertThrows(IllegalArgumentException.class, () -> batch.add(null));
    }
}
