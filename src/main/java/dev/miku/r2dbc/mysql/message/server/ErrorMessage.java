/*
 * Copyright 2018-2021 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

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
        this.errorMessage = requireNonNull(errorMessage, "error message must not be null");
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

    /**
     * Decode error message from a {@link ByteBuf}.
     *
     * @param buf the {@link ByteBuf}.
     * @return decoded error message.
     */
    public static ErrorMessage decode(ByteBuf buf) {
        buf.skipBytes(1); // 0xFF, error message header

        int errorCode = buf.readUnsignedShortLE(); // error code should be unsigned
        String sqlState;

        // Exists only under the protocol 4.1
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

        return errorCode == that.errorCode && Objects.equals(sqlState, that.sqlState) &&
            errorMessage.equals(that.errorMessage);
    }

    @Override
    public int hashCode() {
        int hash = 31 * errorCode + (sqlState != null ? sqlState.hashCode() : 0);
        return 31 * hash + errorMessage.hashCode();
    }

    @Override
    public String toString() {
        return "ErrorMessage{errorCode=" + errorCode + ", sqlState='" + sqlState + "', errorMessage='" +
            errorMessage + "'}";
    }
}
