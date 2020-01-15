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
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link MySqlSyntheticBatch}.
 */
class MySqlSyntheticBatchTest {

    private final MySqlSyntheticBatch batch = new MySqlSyntheticBatch(mock(Client.class), mock(Codecs.class), new ConnectionContext(ZeroDateOption.USE_NULL));

    @SuppressWarnings("ConstantConditions")
    @Test
    void badAdd() {
        assertThrows(IllegalArgumentException.class, () -> batch.add(null));
    }
}
