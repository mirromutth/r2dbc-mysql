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

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link PrepareSimpleStatement}.
 */
class PrepareSimpleStatementTest implements StatementTestSupport<PrepareSimpleStatement> {

    private final Client client = mock(Client.class);

    private final ConnectionContext context = ConnectionContextTest.mock();

    private final Codecs codecs = mock(Codecs.class);

    private final Field fetchSize = PrepareSimpleStatement.class.getDeclaredField("fetchSize");

    PrepareSimpleStatementTest() throws NoSuchFieldException {
        fetchSize.setAccessible(true);
    }

    @Override
    public void badAdd() {
        // No-op
    }

    @Override
    public void bind() {
        // No-op
    }

    @Override
    public void bindNull() {
        // No-op
    }

    @Override
    public int getFetchSize(PrepareSimpleStatement statement) throws IllegalAccessException {
        return fetchSize.getInt(statement);
    }

    @Override
    public PrepareSimpleStatement makeInstance(String parametrizedSql, String simpleSql) {
        return new PrepareSimpleStatement(client, codecs, context, simpleSql, true);
    }

    @Override
    public boolean supportsBinding() {
        return false;
    }
}
