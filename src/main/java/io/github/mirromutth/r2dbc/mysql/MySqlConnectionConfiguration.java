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

import io.github.mirromutth.r2dbc.mysql.constant.SslMode;
import io.github.mirromutth.r2dbc.mysql.constant.ZeroDateOption;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;

import static io.github.mirromutth.r2dbc.mysql.constant.EmptyArrays.EMPTY_STRINGS;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.require;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * MySQL configuration of connection.
 */
public final class MySqlConnectionConfiguration {

    /**
     * Default MySQL port.
     */
    private static final int DEFAULT_PORT = 3306;

    private final String host;

    private final int port;

    @Nullable
    private final Duration connectTimeout;

    private final MySqlSslConfiguration ssl;

    private final ZeroDateOption zeroDateOption;

    private final String username;

    @Nullable
    private final CharSequence password;

    private final String database;

    private MySqlConnectionConfiguration(
        String host, int port, @Nullable Duration connectTimeout, @Nullable MySqlSslConfiguration ssl,
        ZeroDateOption zeroDateOption, String username, @Nullable CharSequence password, @Nullable String database
    ) {
        this.host = requireNonNull(host, "host must not be null");
        this.port = port;
        this.connectTimeout = connectTimeout;
        this.ssl = requireNonNull(ssl, "ssl must not be null");
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.username = requireNonNull(username, "username must not be null");
        this.password = password;
        this.database = database == null || database.isEmpty() ? "" : database;
    }

    public static Builder builder() {
        return new Builder();
    }

    String getHost() {
        return host;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlConnectionConfiguration)) {
            return false;
        }
        MySqlConnectionConfiguration that = (MySqlConnectionConfiguration) o;
        return port == that.port &&
            host.equals(that.host) &&
            Objects.equals(connectTimeout, that.connectTimeout) &&
            ssl.equals(that.ssl) &&
            zeroDateOption == that.zeroDateOption &&
            username.equals(that.username) &&
            Objects.equals(password, that.password) &&
            database.equals(that.database);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, connectTimeout, ssl, zeroDateOption, username, password, database);
    }

    @Override
    public String toString() {
        return String.format("MySqlConnectionConfiguration{host='%s', port=%d, connectTimeout=%s, ssl=%s, zeroDateOption=%s, username='%s', password=REDACTED, database='%s'}",
            host, port, connectTimeout, ssl, zeroDateOption, username, database);
    }

    public static final class Builder {

        @Nullable
        private String database;

        private String host;

        @Nullable
        private CharSequence password;

        private int port = DEFAULT_PORT;

        @Nullable
        private Duration connectTimeout;

        private String username;

        private ZeroDateOption zeroDateOption = ZeroDateOption.USE_NULL;

        private SslMode sslMode = SslMode.PREFERRED;

        private String[] tlsVersion = EMPTY_STRINGS;

        @Nullable
        private String sslCa;

        @Nullable
        private String sslKey;

        @Nullable
        private CharSequence sslKeyPassword;

        @Nullable
        private String sslCert;

        private Builder() {
        }

        public MySqlConnectionConfiguration build() {
            MySqlSslConfiguration ssl = new MySqlSslConfiguration(sslMode, tlsVersion, sslCa, sslKey, sslKeyPassword, sslCert);
            return new MySqlConnectionConfiguration(host, port, connectTimeout, ssl, zeroDateOption, username, password, database);
        }

        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        public Builder host(String host) {
            this.host = requireNonNull(host, "host must not be null");
            return this;
        }

        public Builder password(@Nullable CharSequence password) {
            this.password = password;
            return this;
        }

        public Builder port(int port) {
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

        public Builder sslCa(String sslCa) {
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
    }
}
