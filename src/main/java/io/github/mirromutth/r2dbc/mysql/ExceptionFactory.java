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
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import reactor.util.annotation.Nullable;

/**
 * Factory for SQL Server-specific {@link R2dbcException}s.
 */
final class ExceptionFactory {

    static R2dbcException createException(ErrorMessage message, @Nullable String sql) {
        switch (message.getErrorCode()) {
            case 1054:
            case 1064:
            case 1146:
                return new R2dbcBadGrammarException(message.getErrorMessage(), message.getSqlState(), message.getErrorCode(), sql);
            case 1062: // duplicate key
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
                return new R2dbcDataIntegrityViolationException(message.getErrorMessage(), message.getSqlState(), message.getErrorCode());
        }

        return new R2dbcPermissionDeniedException();
    }
}
