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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link TextSimpleStatement}
 */
class TextSimpleStatementTest {

    private final Client client = mock(Client.class);

    private final ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL);

    private final Codecs codecs = mock(Codecs.class);

    @Test
    void badFetchSize() {
        MySqlStatement statement = new TextSimpleStatement(client, codecs, context, "");

        assertThrows(IllegalArgumentException.class, () -> statement.fetchSize(-1));
        assertThrows(IllegalArgumentException.class, () -> statement.fetchSize(-10));
        assertThrows(IllegalArgumentException.class, () -> statement.fetchSize(Integer.MIN_VALUE));
    }
}
