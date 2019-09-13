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

package dev.miku.r2dbc.mysql.message.server;

import dev.miku.r2dbc.mysql.internal.AssertUtils;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * MySQL error message, sql state will be a property independently.
 */
public final class ErrorMessage implements ServerMessage {

    private static final int SQL_STATE_SIZE = 5;

    private final int errorCode;

    @Nullable
    private final String sqlState;

    private final String errorMessage;

    private ErrorMessage(int errorCode, @Nullable String sqlState, String errorMessage) {
        this.errorCode = errorCode;
        this.sqlState = sqlState;
        this.errorMessage = AssertUtils.requireNonNull(errorMessage, "error message must not be null");
    }

    public int getErrorCode() {
        return errorCode;
    }

    @Nullable
    public String getSqlState() {
        return sqlState;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public static ErrorMessage decode(ByteBuf buf) {
        buf.skipBytes(1); // 0xFF, error message header
        int errorCode = buf.readUnsignedShortLE(); // error code should be unsigned

        String sqlState;

        if ('#' == buf.getByte(buf.readerIndex())) {
            buf.skipBytes(1); // constant '#'
            sqlState = buf.toString(buf.readerIndex(), SQL_STATE_SIZE, StandardCharsets.US_ASCII);
            buf.skipBytes(SQL_STATE_SIZE); // skip fixed string length by read
        } else {
            sqlState = null;
        }

        return new ErrorMessage(errorCode, sqlState, buf.toString(StandardCharsets.US_ASCII));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ErrorMessage)) {
            return false;
        }

        ErrorMessage that = (ErrorMessage) o;

        if (errorCode != that.errorCode) {
            return false;
        }
        if (!Objects.equals(sqlState, that.sqlState)) {
            return false;
        }

        return errorMessage.equals(that.errorMessage);
    }

    @Override
    public int hashCode() {
        int result = errorCode;
        result = 31 * result + (sqlState != null ? sqlState.hashCode() : 0);
        result = 31 * result + errorMessage.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("ErrorMessage{errorCode=%d, sqlState='%s', errorMessage='%s'}", errorCode, sqlState, errorMessage);
    }
}
