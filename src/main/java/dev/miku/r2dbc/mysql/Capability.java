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

package dev.miku.r2dbc.mysql;

/**
 * A capabilities flag bitmap considers to define the session behaviors of the connection.
 * <p>
 * Some flags should not be disabled by driver or always enabled. These flags may have no getter or builder
 * setter, or neither.
 */
public final class Capability {

    /**
     * Can use long password.
     */
    private static final int LONG_PASSWORD = 1;

    /**
     * Use found/touched rows instead of changed rows for affected rows. Should enable it by default.
     */
    private static final int FOUND_ROWS = 2;

    /**
     * Use 2-bytes definition flags of {@code DefinitionMetadataMessage}.
     * <p>
     * Very old servers (before 3.23) will not set this capability flag.
     */
    private static final int LONG_FLAG = 4;

    /**
     * Connect to server with a database.
     */
    private static final int CONNECT_WITH_DB = 8;

    /**
     * Enable it to disallow a statement which use {@code database.table.column} for access other schema
     * data.
     */
    private static final int NO_SCHEMA = 16;

    /**
     * The deflate compression, old compression flag.
     */
    private static final int COMPRESS = 32;

//    private static final int ODBC = 64; // R2DBC driver is not ODBC driver.

    /**
     * Allow to use LOAD DATA [LOCAL] INFILE statement.
     */
    private static final int LOCAL_FILES = 128;

    /**
     * Ignore space between a built-in function name and the subsequent parenthesis.
     * <p>
     * Note: Ignoring spaces may cause ambiguity.
     * <p>
     * See also https://dev.mysql.com/doc/refman/8.0/en/function-resolution.html .
     */
    private static final int IGNORE_SPACE = 256;

    /**
     * The protocol version is 4.1 (instead of 3.20).
     */
    private static final int PROTOCOL_41 = 512;

    /**
     * Use {@code wait_interactive_timeout} instead of {@code wait_timeout} for the server waits for activity
     * on a connection before closing it.
     */
    private static final int INTERACTIVE = 1024;

    /**
     * Enable SSL.
     */
    private static final int SSL = 2048;

//    private static final int IGNORE_SIGPIPE = 4096; // Connector/C only flag.

    /**
     * Allow transactions. All available versions of MySQL server support it.
     */
    private static final int TRANSACTIONS = 8192;

    // Old flag and alias of PROTOCOL_41. It will not be used by any available server version/edition.
//    private static final int RESERVED = 16384;

    /**
     * Allow second part of authentication hashing salt.
     * <p>
     * Old name: RESERVED2.
     * <p>
     * Origin name: SECURE_CONNECTION.
     */
    private static final int SECURE_SALT = 32768;

    /**
     * Allow to send multiple statements in text query and prepare query.
     * <p>
     * Old name: MULTI_QUERIES.
     */
    private static final int MULTI_STATEMENTS = 65536;

    /**
     * Allow to receive multiple results in the response of executing a text query.
     */
    private static final int MULTI_RESULTS = 1 << 17;

    /**
     * Allow to receive multiple results in the response of executing a prepare query.
     */
    private static final int PS_MULTI_RESULTS = 1 << 18;

    /**
     * Supports authentication plugins. Server will send more details (i.e. name) for authentication plugin.
     */
    private static final int PLUGIN_AUTH = 1 << 19;

    /**
     * Connection attributes should be sent.
     */
    private static final int CONNECT_ATTRS = 1 << 20;

    /**
     * Can use var-integer sized bytes to encode client authentication.
     * <p>
     * Origin name: PLUGIN_AUTH_LENENC_CLIENT_DATA.
     */
    private static final int VAR_INT_SIZED_AUTH = 1 << 21;

//    private static final int HANDLE_EXPIRED_PASSWORD = 1 << 22; // Client can handle expired passwords.
//    private static final int SESSION_TRACK = 1 << 23;

    /**
     * The MySQL server marks the EOF message as deprecated and use OK message instead.
     */
    private static final int DEPRECATE_EOF = 1 << 24;

    // Allow the server not to send column metadata in result set,
    // should NEVER enable this option.
//    private static final int OPTIONAL_RESULT_SET_METADATA = 1 << 25;
//    private static final int Z_STD_COMPRESSION = 1 << 26;

    // A reserved flag, used to extend the 32-bits capability bitmap to 64-bits.
    // There is no available MySql server version/edition to support it.
//    private static final int CAPABILITY_EXTENSION = 1 << 29;
//    private static final int SSL_VERIFY_SERVER_CERT = 1 << 30; // Client only flag, use SslMode instead.
//    private static final int REMEMBER_OPTIONS = 1 << 31; // Connector/C only flag.

    private static final int ALL_SUPPORTED = LONG_PASSWORD | FOUND_ROWS | LONG_FLAG | CONNECT_WITH_DB |
        NO_SCHEMA | COMPRESS | LOCAL_FILES | IGNORE_SPACE | PROTOCOL_41 | INTERACTIVE | SSL |
        TRANSACTIONS | SECURE_SALT | MULTI_STATEMENTS | MULTI_RESULTS | PS_MULTI_RESULTS |
        PLUGIN_AUTH | CONNECT_ATTRS | VAR_INT_SIZED_AUTH | DEPRECATE_EOF;

    private final int bitmap;

    /**
     * Checks if the connection will be connected and logon with a database.
     *
     * @return if login with database.
     */
    public boolean isConnectWithDatabase() {
        return (bitmap & CONNECT_WITH_DB) != 0;
    }

    /**
     * Checks if the connection enabled SSL.
     *
     * @return if SSL enabled.
     */
    public boolean isSslEnabled() {
        return (bitmap & SSL) != 0;
    }

    /**
     * Checks if the connection is using protocol 4.1.
     *
     * @return if using protocol 4.1.
     */
    public boolean isProtocol41() {
        return (bitmap & PROTOCOL_41) != 0;
    }

    /**
     * Checks if can use var-integer sized bytes to encode client authentication.
     *
     * @return if can use var-integer sized authentication.
     */
    public boolean isVarIntSizedAuthAllowed() {
        return (bitmap & VAR_INT_SIZED_AUTH) != 0;
    }

    /**
     * Checks if allow authentication plugin type name.
     *
     * @return if allowed.
     */
    public boolean isPluginAuthAllowed() {
        return (bitmap & PLUGIN_AUTH) != 0;
    }

    /**
     * Checks if the connection contains connection attributes.
     *
     * @return if has connection attributes.
     */
    public boolean isConnectionAttributesAllowed() {
        return (bitmap & CONNECT_ATTRS) != 0;
    }

    /**
     * Checks if server supports multiple-statement. i.e. computed statement.
     *
     * @return if server supported.
     */
    public boolean isMultiStatementsAllowed() {
        return (bitmap & MULTI_STATEMENTS) != 0;
    }

    /**
     * Checks if server marks EOF message as deprecated.
     *
     * @return if EOF message was deprecated.
     */
    public boolean isEofDeprecated() {
        return (bitmap & DEPRECATE_EOF) != 0;
    }

    /**
     * Checks if server uses more than 8 bytes of salt.
     *
     * @return if using secure salt.
     */
    public boolean isSaltSecured() {
        return (bitmap & SECURE_SALT) != 0;
    }

    /**
     * Checks if server supports transaction.
     *
     * @return if server supported.
     */
    public boolean isTransactionAllowed() {
        return (bitmap & TRANSACTIONS) != 0;
    }

    /**
     * Get the original bitmap of {@link Capability this}.
     *
     * @return the bitmap.
     */
    public int getBitmap() {
        return bitmap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Capability)) {
            return false;
        }

        Capability that = (Capability) o;

        return bitmap == that.bitmap;
    }

    @Override
    public int hashCode() {
        return bitmap;
    }

    @Override
    public String toString() {
        // Do not consider complex output, just use hex.
        return "Capability<0x" + Integer.toHexString(bitmap) + '>';
    }

    Builder mutate() {
        return new Builder(bitmap);
    }

    private Capability(int bitmap) {
        this.bitmap = bitmap;
    }

    /**
     * Creates a {@link Capability} with capabilities bitmap. It will unset all unknown flags.
     *
     * @param capabilities the capabilities bitmap.
     * @return the {@link Capability} without unknown flags.
     */
    public static Capability of(int capabilities) {
        return new Capability(capabilities & ALL_SUPPORTED);
    }

    static final class Builder {

        private int bitmap;

        void disableConnectWithDatabase() {
            this.bitmap &= ~CONNECT_WITH_DB;
        }

        void disableDatabasePinned() {
            this.bitmap &= ~NO_SCHEMA;
        }

        void disableCompression() {
            this.bitmap &= ~COMPRESS;
        }

        void disableLoadDataInfile() {
            this.bitmap &= ~LOCAL_FILES;
        }

        void disableIgnoreAmbiguitySpace() {
            this.bitmap &= ~IGNORE_SPACE;
        }

        void disableInteractiveTimeout() {
            this.bitmap &= ~INTERACTIVE;
        }

        void disableSsl() {
            this.bitmap &= ~SSL;
        }

        void disableConnectAttributes() {
            this.bitmap &= ~CONNECT_ATTRS;
        }

        Capability build() {
            return of(this.bitmap);
        }

        private Builder(int bitmap) {
            this.bitmap = bitmap;
        }
    }
}
