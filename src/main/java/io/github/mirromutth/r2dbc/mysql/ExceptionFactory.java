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

import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.netty.util.collection.IntObjectHashMap;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A factory for generate {@link R2dbcException}s.
 */
final class ExceptionFactory {

    static R2dbcException createException(ErrorMessage message, @Nullable String sql) {
        requireNonNull(message, "error message must not be null");

        int errorCode = message.getErrorCode();
        String sqlState = message.getSqlState();
        String errorMessage = message.getErrorMessage();

        R2dbcException exception = mappingErrorCode(errorMessage, sqlState, errorCode, sql);

        if (exception == null && sqlState != null) {
            exception = mappingSqlState(errorMessage, sqlState, errorCode, sql);
        }

        if (exception != null) {
            return exception;
        }

        // Fallback when all exceptions mismatch.
        return new R2dbcNonTransientResourceException(errorMessage, sqlState, errorCode);
    }

    @Nullable
    private static R2dbcException mappingErrorCode(String errorMessage, @Nullable String sqlState, int errorCode, @Nullable String sql) {
        // TODO: support more error codes
        switch (errorCode) {
            case 1213: // Dead lock :-( no one wants this
                return new R2dbcTransientResourceException(errorMessage, sqlState, errorCode);
            case 1205: // Wait lock timeout
                return new R2dbcTimeoutException(errorMessage, sqlState, errorCode);
            case 1054: // Unknown column name in existing table
            case 1064: // Bad syntax
            case 1146: // Unknown table name
            case 1630: // Function not exists
                return new R2dbcBadGrammarException(errorMessage, sqlState, errorCode, sql);
            case 1062: // Duplicate entry key
            case 630:
            case 839:
            case 840:
            case 893:
            case 1169:
            case 1215:
            case 1216:
            case 1217:
            case 1364:
            case 1451:
            case 1452:
            case 1557:
                return new R2dbcDataIntegrityViolationException(errorMessage, sqlState, errorCode);
        }

        return null;
    }

    @Nullable
    private static R2dbcException mappingSqlState(String errorMessage, String sqlState, int errorCode, @Nullable String sql) {
        // TODO: implement mapping by standard sql state rules
        return null;
    }
}
