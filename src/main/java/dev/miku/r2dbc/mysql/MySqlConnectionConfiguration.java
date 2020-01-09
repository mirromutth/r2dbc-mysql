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

import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_STRINGS;
import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL configuration of connection.
 */
public final class MySqlConnectionConfiguration {

    /**
     * Default MySQL port.
     */
    private static final int DEFAULT_PORT = 3306;

    private static final Predicate<String> DEFAULT_SERVER_PREPARE = sql -> false;

    /**
     * {@code true} if {@link #domain} is hostname, otherwise {@link #domain} is unix domain socket path.
     */
    private final boolean isHost;

    /**
     * Domain of connecting, may be hostname or unix domain socket path.
     */
    private final String domain;

    private final int port;

    private final MySqlSslConfiguration ssl;

    @Nullable
    private final Duration connectTimeout;

    private final ZeroDateOption zeroDateOption;

    private final String username;

    @Nullable
    private final CharSequence password;

    private final String database;

    @Nullable
    private final Predicate<String> preferPrepareStatement;

    private MySqlConnectionConfiguration(
        boolean isHost, String domain, int port, @Nullable MySqlSslConfiguration ssl,
        @Nullable Duration connectTimeout, ZeroDateOption zeroDateOption,
        String username, @Nullable CharSequence password, @Nullable String database,
        @Nullable Predicate<String> preferPrepareStatement
    ) {
        this.isHost = isHost;
        this.domain = domain;
        this.port = port;
        this.connectTimeout = connectTimeout;
        this.ssl = requireNonNull(ssl, "ssl must not be null");
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.username = requireNonNull(username, "username must not be null");
        this.password = password;
        this.database = database == null || database.isEmpty() ? "" : database;
        this.preferPrepareStatement = preferPrepareStatement;
    }

    public static Builder builder() {
        return new Builder();
    }

    boolean isHost() {
        return isHost;
    }

    String getDomain() {
        return domain;
    }

    int getPort() {
        return port;
    }

    @Nullable
    Duration getConnectTimeout() {
        return connectTimeout;
    }

    MySqlSslConfiguration getSsl() {
        return ssl;
    }

    ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    String getUsername() {
        return username;
    }

    @Nullable
    CharSequence getPassword() {
        return password;
    }

    String getDatabase() {
        return database;
    }

    @Nullable
    Predicate<String> getPreferPrepareStatement() {
        return preferPrepareStatement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlConnectionConfiguration)) {
            return false;
        }
        MySqlConnectionConfiguration that = (MySqlConnectionConfiguration) o;
        return isHost == that.isHost &&
            domain.equals(that.domain) &&
            port == that.port &&
            ssl.equals(that.ssl) &&
            Objects.equals(connectTimeout, that.connectTimeout) &&
            zeroDateOption == that.zeroDateOption &&
            username.equals(that.username) &&
            Objects.equals(password, that.password) &&
            database.equals(that.database) &&
            Objects.equals(preferPrepareStatement, that.preferPrepareStatement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isHost, domain, port, ssl, connectTimeout, zeroDateOption, username, password, database, preferPrepareStatement);
    }

    @Override
    public String toString() {
        if (isHost) {
            return String.format("MySqlConnectionConfiguration{host=%s, port=%d, ssl=%s, connectTimeout=%s, zeroDateOption=%s, username='%s', password=REDACTED, database='%s', preferPrepareStatement=%s}",
                domain, port, ssl, connectTimeout, zeroDateOption, username, database, preferPrepareStatement);
        } else {
            return String.format("MySqlConnectionConfiguration{unixSocket=%s, connectTimeout=%s, zeroDateOption=%s, username='%s', password=REDACTED, database='%s', preferPrepareStatement=%s}",
                domain, connectTimeout, zeroDateOption, username, database, preferPrepareStatement);
        }
    }

    public static final class Builder {

        @Nullable
        private String database;

        private boolean isHost = true;

        private String domain;

        @Nullable
        private CharSequence password;

        private int port = DEFAULT_PORT;

        @Nullable
        private Duration connectTimeout;

        private String username;

        private ZeroDateOption zeroDateOption = ZeroDateOption.USE_NULL;

        @Nullable
        private SslMode sslMode;

        private String[] tlsVersion = EMPTY_STRINGS;

        @Nullable
        private String sslCa;

        @Nullable
        private String sslKey;

        @Nullable
        private CharSequence sslKeyPassword;

        @Nullable
        private String sslCert;

        @Nullable
        private Predicate<String> preferPrepareStatement;

        private Builder() {
        }

        public MySqlConnectionConfiguration build() {
            SslMode sslMode = requireSslMode();

            if (isHost) {
                requireNonNull(domain, "host must not be null when using TCP socket");
                require(port >= 0 && port <= 0xFFFF, "port must be between 0 and 65535 when using TCP socket");
            } else {
                requireNonNull(domain, "unixSocket must not be null when using unix domain socket");
                require(!sslMode.startSsl(), "sslMode must be disabled when using unix domain socket");
            }

            MySqlSslConfiguration ssl = MySqlSslConfiguration.create(sslMode, tlsVersion, sslCa, sslKey, sslKeyPassword, sslCert);
            return new MySqlConnectionConfiguration(isHost, domain, port, ssl, connectTimeout, zeroDateOption, username, password, database, preferPrepareStatement);
        }

        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        public Builder unixSocket(String unixSocket) {
            this.domain = requireNonNull(unixSocket, "unixSocket must not be null");
            this.isHost = false;
            return this;
        }

        public Builder host(String host) {
            this.domain = requireNonNull(host, "host must not be null");
            this.isHost = true;
            return this;
        }

        public Builder password(@Nullable CharSequence password) {
            this.password = password;
            return this;
        }

        public Builder port(int port) {
            require(port >= 0 && port <= 0xFFFF, "port must be between 0 and 65535");

            this.port = port;
            return this;
        }

        public Builder connectTimeout(@Nullable Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder username(String username) {
            this.username = requireNonNull(username, "username must not be null");
            return this;
        }

        public Builder zeroDateOption(ZeroDateOption zeroDate) {
            this.zeroDateOption = requireNonNull(zeroDate, "zeroDateOption must not be null");
            return this;
        }

        public Builder sslMode(SslMode sslMode) {
            this.sslMode = requireNonNull(sslMode, "sslMode must not be null");
            return this;
        }

        public Builder tlsVersion(String... tlsVersion) {
            requireNonNull(tlsVersion, "tlsVersion must not be null");

            int size = tlsVersion.length;

            if (size > 0) {
                String[] versions = new String[size];
                System.arraycopy(tlsVersion, 0, versions, 0, size);
                this.tlsVersion = versions;
            } else {
                this.tlsVersion = EMPTY_STRINGS;
            }
            return this;
        }

        public Builder sslCa(@Nullable String sslCa) {
            this.sslCa = sslCa;
            return this;
        }

        public Builder sslKeyAndCert(@Nullable String sslCert, @Nullable String sslKey) {
            return sslKeyAndCert(sslCert, sslKey, null);
        }

        public Builder sslKeyAndCert(@Nullable String sslCert, @Nullable String sslKey, @Nullable CharSequence sslKeyPassword) {
            require((sslCert == null && sslKey == null) || (sslCert != null && sslKey != null), "SSL key and cert must be both null or both non-null");

            this.sslCert = sslCert;
            this.sslKey = sslKey;
            this.sslKeyPassword = sslKeyPassword;
            return this;
        }

        public Builder useClientPrepareStatement() {
            this.preferPrepareStatement = null;
            return this;
        }

        public Builder useServerPrepareStatement() {
            return useServerPrepareStatement(DEFAULT_SERVER_PREPARE);
        }

        public Builder useServerPrepareStatement(Predicate<String> preferPrepareStatement) {
            requireNonNull(preferPrepareStatement, "preferPrepareStatement must not be null");

            this.preferPrepareStatement = preferPrepareStatement;
            return this;
        }

        private SslMode requireSslMode() {
            SslMode sslMode = this.sslMode;

            if (sslMode == null) {
                sslMode = isHost ? SslMode.PREFERRED : SslMode.DISABLED;
            }

            return sslMode;
        }
    }
}
