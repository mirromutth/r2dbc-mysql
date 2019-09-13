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

import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import reactor.util.annotation.Nullable;

import static dev.miku.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * A factory for generate {@link R2dbcException}s.
 */
final class ExceptionFactory {

    private static final String CONSTRAINT_VIOLATION_PREFIX = "23";

    private static final String TRANSACTION_ROLLBACK_PREFIX = "40";

    private static final String SYNTAX_ERROR_PREFIX = "42";

    static R2dbcException createException(ErrorMessage message, @Nullable String sql) {
        requireNonNull(message, "error message must not be null");

        int errorCode = message.getErrorCode();
        String sqlState = message.getSqlState();
        String errorMessage = message.getErrorMessage();

        // Should keep looking more error codes
        switch (errorCode) {
            case 1044: // Database access denied
            case 1045: // Wrong password
            case 1095: // Kill thread denied
            case 1142: // Table access denied
            case 1143: // Column access denied
            case 1227: // Operation has no privilege(s)
            case 1370: // Routine or process access denied
            case 1698: // User need password but has no password
            case 1873: // Change user denied
                return new R2dbcPermissionDeniedException(errorMessage, sqlState, errorCode);
            case 1159: // Read interrupted, reading basic packet timeout because of network jitter in most cases
            case 1161: // Write interrupted, writing basic packet timeout because of network jitter in most cases
            case 1213: // Dead lock :-( no one wants this
            case 1317: // Statement execution interrupted
                return new R2dbcTransientResourceException(errorMessage, sqlState, errorCode);
            case 1205: // Wait lock timeout
            case 1907: // Statement executing timeout
                return new R2dbcTimeoutException(errorMessage, sqlState, errorCode);
            case 1613: // Transaction rollback because of took too long
                return new R2dbcRollbackException(errorMessage, sqlState, errorCode);
            case 1050: // Table already exists
            case 1051: // Unknown table
            case 1054: // Unknown column name in existing table
            case 1064: // Bad syntax
            case 1247: // Unsupported reference
            case 1146: // Unknown table name
            case 1304: // Something already exists, like savepoint
            case 1305: // Something does not exists, like savepoint
            case 1630: // Function not exists
                return new R2dbcBadGrammarException(errorMessage, sqlState, errorCode, sql);
            case 1022: // Duplicate key
            case 1048: // Field cannot be null
            case 1062: // Duplicate entry for key constraint
            case 1169: // Violation of an unique constraint
            case 1215: // Add a foreign key has a violation
            case 1216: // Child row has a violation of foreign key constraint when inserting or updating
            case 1217: // Parent row has a violation of foreign key constraint when deleting or updating
            case 1364: // Field has no default value but user try set it to DEFAULT
            case 1451: // Parent row has a violation of foreign key constraint when deleting or updating
            case 1452: // Child row has a violation of foreign key constraint when inserting or updating
            case 1557: // Conflicting foreign key constraints and unique constraints
            case 1859: // Duplicate unknown entry for key constraint
                return new R2dbcDataIntegrityViolationException(errorMessage, sqlState, errorCode);
        }

        if (sqlState == null) {
            // Has no SQL state, all exceptions mismatch, fallback.
            return new R2dbcNonTransientResourceException(errorMessage, null, errorCode);
        }

        return mappingSqlState(errorMessage, sqlState, errorCode, sql);
    }

    private static R2dbcException mappingSqlState(String errorMessage, String sqlState, int errorCode, @Nullable String sql) {
        if (sqlState.startsWith(SYNTAX_ERROR_PREFIX)) {
            return new R2dbcBadGrammarException(errorMessage, sqlState, errorCode, sql);
        } else if (sqlState.startsWith(CONSTRAINT_VIOLATION_PREFIX)) {
            return new R2dbcDataIntegrityViolationException(errorMessage, sqlState, errorCode);
        } else if (sqlState.startsWith(TRANSACTION_ROLLBACK_PREFIX)) {
            return new R2dbcRollbackException(errorMessage, sqlState, errorCode);
        }

        // Uncertain SQL state, all exceptions mismatch, fallback.
        return new R2dbcNonTransientResourceException(errorMessage, null, errorCode);
    }
}
