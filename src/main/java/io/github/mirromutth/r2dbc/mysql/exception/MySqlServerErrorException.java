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

package io.github.mirromutth.r2dbc.mysql.exception;

import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.ErrorMessage;
import io.r2dbc.spi.R2dbcException;
import reactor.core.publisher.SynchronousSink;

/**
 * The exception just wrap error message from MySQL server.
 */
public final class MySqlServerErrorException extends R2dbcException {

    private MySqlServerErrorException(String reason, String sqlState, int errorCode) {
        super(reason, sqlState, errorCode);
    }

    public static void handleErrorResponse(BackendMessage message, SynchronousSink<BackendMessage> sink) {
        if (message instanceof ErrorMessage) {
            ErrorMessage error = (ErrorMessage) message;
            sink.error(new MySqlServerErrorException(error.getErrorMessage(), error.getSqlState(), error.getErrorCode()));
        } else {
            sink.next(message);
        }
    }
}
