/*
 * Copyright 2019 the original author or authors.
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
import io.r2dbc.spi.R2dbcBadGrammarException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static io.github.mirromutth.r2dbc.mysql.MySqlConnectionRunner.completeAll;
import static io.github.mirromutth.r2dbc.mysql.MySqlConnectionRunner.exceptAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link MySqlConnection}.
 */
class MySqlConnectionTest {

    private static final String SYNTAX_ERROR = "42000";

    private static final int ERROR_SOMEONE_NOT_FOUND = 1305;

    @Test
    void beginTransaction() {
        completeAll(connection -> Mono.<Void>fromRunnable(() -> assertTrue(connection.isAutoCommit()))
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertFalse(connection.isAutoCommit()))
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertFalse(connection.isAutoCommit())));
    }

    @Test
    void commitTransactionWithoutBegin() {
        completeAll(MySqlConnection::commitTransaction);
    }

    @Test
    void rollbackTransactionWithoutBegin() {
        completeAll(MySqlConnection::rollbackTransaction);
    }

    @Test
    void rejectInvalidSavepoint() {
        completeAll(connection -> {
            assertThrows(IllegalArgumentException.class, () -> connection.createSavepoint(""));
            assertThrows(IllegalArgumentException.class, () -> connection.createSavepoint("`"));
            return Mono.empty();
        });
    }

    @Test
    void createSavepoint() {
        completeAll(connection -> connection.createSavepoint("foo"));
    }

    @Test
    void releaseSavepointWithoutTransaction() {
        // MySQL treats savepoint not found as a syntax error (SQL state: 42000).
        exceptAll(
            R2dbcBadGrammarException.class,
            connection -> connection.releaseSavepoint("foo"),
            e -> {
                assertTrue(e instanceof R2dbcBadGrammarException);
                R2dbcBadGrammarException r2dbcExcept = (R2dbcBadGrammarException) e;
                String sqlState = r2dbcExcept.getSqlState();

                assertEquals(r2dbcExcept.getErrorCode(), ERROR_SOMEONE_NOT_FOUND);
                assertNotNull(sqlState);
                assertEquals(sqlState, SYNTAX_ERROR);
                assertEquals(r2dbcExcept.getOffendingSql(), "RELEASE SAVEPOINT `foo`");
            }
        );
    }

    @Test
    void rollbackTransactionToSavepointWithoutTransaction() {
        // MySQL treats savepoint not found as a syntax error (SQL state: 42000).
        exceptAll(
            R2dbcBadGrammarException.class,
            connection -> connection.rollbackTransactionToSavepoint("foo"),
            e -> {
                assertTrue(e instanceof R2dbcBadGrammarException);
                R2dbcBadGrammarException r2dbcExcept = (R2dbcBadGrammarException) e;
                String sqlState = r2dbcExcept.getSqlState();

                assertEquals(r2dbcExcept.getErrorCode(), ERROR_SOMEONE_NOT_FOUND);
                assertNotNull(sqlState);
                assertEquals(sqlState, SYNTAX_ERROR);
                assertEquals(r2dbcExcept.getOffendingSql(), "ROLLBACK TO SAVEPOINT `foo`");
            }
        );
    }

    @Test
    void setTransactionIsolationLevel() {
        completeAll(connection -> connection.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED));
    }

    @Test
    void ping() {
        completeAll(MySqlConnection::ping);
    }
}
