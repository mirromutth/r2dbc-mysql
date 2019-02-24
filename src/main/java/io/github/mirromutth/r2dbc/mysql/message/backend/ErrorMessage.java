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

package io.github.mirromutth.r2dbc.mysql.message.backend;

import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL error message, sql state will be a property independently.
 */
public final class ErrorMessage implements BackendMessage {

    private static final int SQL_STATE_SIZE = 5;

    private final short errorCode;

    private final String sqlState;

    private final String errorMessage;

    private ErrorMessage(short errorCode, String sqlState, String errorMessage) {
        this.errorCode = errorCode;
        this.sqlState = requireNonNull(sqlState, "sqlState must not be null");
        this.errorMessage = requireNonNull(errorMessage, "errorMessage must not be null");
    }

    public short getErrorCode() {
        return errorCode;
    }

    public String getSqlState() {
        return sqlState;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    static ErrorMessage decode(ByteBuf buf) {
        buf.skipBytes(1); // 0xFF, error message header
        short errorCode = buf.readShortLE();
        buf.skipBytes(1); // constant '#'
        String sqlState = buf.toString(buf.readerIndex(), SQL_STATE_SIZE, StandardCharsets.US_ASCII);
        buf.skipBytes(SQL_STATE_SIZE); // skip fixed string length by read
        return new ErrorMessage(errorCode, sqlState, CodecUtils.readCString(buf, StandardCharsets.US_ASCII));
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
        if (!sqlState.equals(that.sqlState)) {
            return false;
        }
        return errorMessage.equals(that.errorMessage);
    }

    @Override
    public int hashCode() {
        int result = (int) errorCode;
        result = 31 * result + sqlState.hashCode();
        result = 31 * result + errorMessage.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ErrorMessage{" +
            "errorCode=" + errorCode +
            ", sqlState='" + sqlState + '\'' +
            ", errorMessage='" + errorMessage + '\'' +
            '}';
    }
}
