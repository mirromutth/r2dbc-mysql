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
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A message considers an error that's reported by server-side. Not like JDBC MySQL, the SQL state is an
 * independent property.
 * <p>
 * The {@link #offendingSql} will be bound by statement flow, protocol layer will always get {@code null}.
 */
public final class ErrorMessage implements ServerMessage {

    private static final String CONSTRAINT_VIOLATION_PREFIX = "23";

    private static final String TRANSACTION_ROLLBACK_PREFIX = "40";

    private static final String SYNTAX_ERROR_PREFIX = "42";

    private static final int SQL_STATE_SIZE = 5;

    private final int code;

    @Nullable
    private final String sqlState;

    private final String message;

    @Nullable
    private final String offendingSql;

    private ErrorMessage(int code, @Nullable String sqlState, String message) {
        this(code, sqlState, message, null);
    }

    private ErrorMessage(int code, @Nullable String sqlState, String message, @Nullable String offendingSql) {
        this.code = code;
        this.sqlState = sqlState;
        this.message = requireNonNull(message, "message must not be null");
        this.offendingSql = offendingSql;
    }

    public R2dbcException toException() {
        return toException(offendingSql);
    }

    public R2dbcException toException(@Nullable String sql) {
        // Should keep looking more error codes
        switch (code) {
            case 1044: // Database access denied
            case 1045: // Wrong password
            case 1095: // Kill thread denied
            case 1142: // Table access denied
            case 1143: // Column access denied
            case 1227: // Operation has no privilege(s)
            case 1370: // Routine or process access denied
            case 1698: // User need password but has no password
            case 1873: // Change user denied
                return new R2dbcPermissionDeniedException(message, sqlState, code);
            case 1159: // Read interrupted, reading packet timeout because of network jitter in most cases
            case 1161: // Write interrupted, writing packet timeout because of network jitter in most cases
            case 1213: // Dead-lock :-( no one wants this
            case 1317: // Statement execution interrupted
                return new R2dbcTransientResourceException(message, sqlState, code);
            case 1205: // Wait lock timeout
            case 1907: // Statement executing timeout
                return new R2dbcTimeoutException(message, sqlState, code);
            case 1613: // Transaction rollback because of took too long
                return new R2dbcRollbackException(message, sqlState, code);
            case 1050: // Table already exists
            case 1051: // Unknown table
            case 1054: // Unknown column name in existing table
            case 1064: // Bad syntax
            case 1247: // Unsupported reference
            case 1146: // Unknown table name
            case 1304: // Something already exists, like savepoint
            case 1305: // Something does not exists, like savepoint
            case 1630: // Function not exists
                return new R2dbcBadGrammarException(message, sqlState, code, sql);
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
                return new R2dbcDataIntegrityViolationException(message, sqlState, code);
        }

        if (sqlState == null) {
            // Has no SQL state, all exceptions mismatch, fallback.
            return new R2dbcNonTransientResourceException(message, null, code);
        } else if (sqlState.startsWith(SYNTAX_ERROR_PREFIX)) {
            return new R2dbcBadGrammarException(message, sqlState, code, sql);
        } else if (sqlState.startsWith(CONSTRAINT_VIOLATION_PREFIX)) {
            return new R2dbcDataIntegrityViolationException(message, sqlState, code);
        } else if (sqlState.startsWith(TRANSACTION_ROLLBACK_PREFIX)) {
            return new R2dbcRollbackException(message, sqlState, code);
        }

        // Uncertain SQL state, all exceptions mismatch, fallback.
        return new R2dbcNonTransientResourceException(message, null, code);
    }

    public int getCode() {
        return code;
    }

    @Nullable
    public String getSqlState() {
        return sqlState;
    }

    public String getMessage() {
        return message;
    }

    /**
     * Creates a new {@link ErrorMessage} with specific offending statement.
     *
     * @param sql offending statement.
     * @return {@code this} if {@code sql} is {@code null}, otherwise a new {@link ErrorMessage}.
     */
    public ErrorMessage offendedBy(@Nullable String sql) {
        return sql == null ? this : new ErrorMessage(code, sqlState, message, sql);
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

        return code == that.code && Objects.equals(sqlState, that.sqlState) &&
            message.equals(that.message) && Objects.equals(offendingSql, that.offendingSql);
    }

    @Override
    public int hashCode() {
        int hash = 31 * code + Objects.hashCode(sqlState);
        return 31 * (31 * hash + message.hashCode()) + Objects.hashCode(offendingSql);
    }

    @Override
    public String toString() {
        return "ErrorMessage{code=" + code + ", sqlState='" + sqlState + "', message='" + message + "'}";
    }
}
