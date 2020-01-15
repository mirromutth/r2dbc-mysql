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
import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link MySqlConnection}.
 */
class MySqlConnectionTest {

    private final Client client = mock(Client.class);

    private final Codecs codecs = mock(Codecs.class);

    private final IsolationLevel level = IsolationLevel.REPEATABLE_READ;

    private final String product = "MockConnection";

    private final MySqlConnection connection = new MySqlConnection(client, mockContext(), codecs, level, product, null);

    @Test
    void createStatement() {
        String condition = "SELECT * FROM test";
        MySqlConnection allPrepare = new MySqlConnection(client, mockContext(), codecs, level, product, sql -> true);
        MySqlConnection halfPrepare = new MySqlConnection(client, mockContext(), codecs, level, product, sql -> false);
        MySqlConnection conditionPrepare = new MySqlConnection(client, mockContext(), codecs, level, product, sql -> sql.equals(condition));

        assertThat(connection.createStatement("SELECT * FROM test WHERE id=1")).isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(connection.createStatement(condition)).isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(connection.createStatement("SELECT * FROM test WHERE id=?")).isExactlyInstanceOf(TextParametrizedStatement.class);

        assertThat(allPrepare.createStatement("SELECT * FROM test WHERE id=1")).isExactlyInstanceOf(PrepareSimpleStatement.class);
        assertThat(allPrepare.createStatement(condition)).isExactlyInstanceOf(PrepareSimpleStatement.class);
        assertThat(allPrepare.createStatement("SELECT * FROM test WHERE id=?")).isExactlyInstanceOf(PrepareParametrizedStatement.class);

        assertThat(halfPrepare.createStatement("SELECT * FROM test WHERE id=1")).isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(halfPrepare.createStatement(condition)).isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(halfPrepare.createStatement("SELECT * FROM test WHERE id=?")).isExactlyInstanceOf(PrepareParametrizedStatement.class);

        assertThat(conditionPrepare.createStatement("SELECT * FROM test WHERE id=1")).isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(conditionPrepare.createStatement(condition)).isExactlyInstanceOf(PrepareSimpleStatement.class);
        assertThat(conditionPrepare.createStatement("SELECT * FROM test WHERE id=?")).isExactlyInstanceOf(PrepareParametrizedStatement.class);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badCreateStatement() {
        assertThrows(IllegalArgumentException.class, () -> connection.createStatement(null));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badCreateSavepoint() {
        assertThrows(IllegalArgumentException.class, () -> connection.createSavepoint(""));
        assertThrows(IllegalArgumentException.class, () -> connection.createSavepoint("`"));
        assertThrows(IllegalArgumentException.class, () -> connection.createSavepoint("name`"));
        assertThrows(IllegalArgumentException.class, () -> connection.createSavepoint("nam`e"));
        assertThrows(IllegalArgumentException.class, () -> connection.createSavepoint(null));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badReleaseSavepoint() {
        assertThrows(IllegalArgumentException.class, () -> connection.releaseSavepoint(""));
        assertThrows(IllegalArgumentException.class, () -> connection.releaseSavepoint("`"));
        assertThrows(IllegalArgumentException.class, () -> connection.releaseSavepoint("name`"));
        assertThrows(IllegalArgumentException.class, () -> connection.releaseSavepoint("nam`e"));
        assertThrows(IllegalArgumentException.class, () -> connection.releaseSavepoint(null));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badRollbackTransactionToSavepoint() {
        assertThrows(IllegalArgumentException.class, () -> connection.rollbackTransactionToSavepoint(""));
        assertThrows(IllegalArgumentException.class, () -> connection.rollbackTransactionToSavepoint("`"));
        assertThrows(IllegalArgumentException.class, () -> connection.rollbackTransactionToSavepoint("name`"));
        assertThrows(IllegalArgumentException.class, () -> connection.rollbackTransactionToSavepoint("nam`e"));
        assertThrows(IllegalArgumentException.class, () -> connection.rollbackTransactionToSavepoint(null));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badSetTransactionIsolationLevel() {
        assertThrows(IllegalArgumentException.class, () -> connection.setTransactionIsolationLevel(null));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badValidate() {
        assertThrows(IllegalArgumentException.class, () -> connection.validate(null));
    }

    private static ConnectionContext mockContext() {
        ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL);
        context.setConnectionId(1);
        context.setCapabilities(Capabilities.ALL_SUPPORTED);
        context.setServerStatuses(ServerStatuses.AUTO_COMMIT);
        return context;
    }
}
