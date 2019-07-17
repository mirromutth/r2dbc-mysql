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

import io.github.mirromutth.r2dbc.mysql.constant.ZeroDateOption;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.function.Consumer;

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

    @Nullable
    private final MySqlSslConfiguration sslConfiguration;

    private final ZeroDateOption zeroDateOption;

    private final String username;

    private final CharSequence password;

    private final String database;

    private MySqlConnectionConfiguration(
        String host, int port, @Nullable Duration connectTimeout, @Nullable MySqlSslConfiguration sslConfiguration,
        ZeroDateOption zeroDateOption, String username, @Nullable CharSequence password, @Nullable String database
    ) {
        this.host = requireNonNull(host, "host must not be null");
        this.port = port;
        this.connectTimeout = connectTimeout;
        this.sslConfiguration = sslConfiguration;
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.username = requireNonNull(username, "username must not be null");
        this.password = password;

        if (database == null || database.isEmpty()) {
            this.database = "";
        } else {
            this.database = database; // or use `database.intern()`?
        }
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

    @Nullable
    MySqlSslConfiguration getSslConfiguration() {
        return sslConfiguration;
    }

    ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    String getUsername() {
        return username;
    }

    CharSequence getPassword() {
        return password;
    }

    String getDatabase() {
        return database;
    }

    @Override
    public String toString() {
        return "MySqlConnectionConfiguration{" +
            "host='" + host + '\'' +
            ", port=" + port +
            ", connectTimeout=" + connectTimeout +
            ", sslConfiguration=" + (sslConfiguration == null ? "disabled" : sslConfiguration.toString()) +
            ", zeroDateOption=" + zeroDateOption +
            ", username='" + username + '\'' +
            ", password=REDACTED" +
            ", database='" + database + '\'' +
            '}';
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

        @Nullable
        private MySqlSslConfiguration sslConfiguration;

        private String username;

        private ZeroDateOption zeroDateOption = ZeroDateOption.USE_NULL;

        private Builder() {
        }

        public MySqlConnectionConfiguration build() {
            return new MySqlConnectionConfiguration(host, port, connectTimeout, sslConfiguration, zeroDateOption, username, password, database);
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

        public Builder enableSsl(Consumer<MySqlSslConfiguration.Builder> options) {
            MySqlSslConfiguration.Builder builder = MySqlSslConfiguration.builder();
            options.accept(builder);

            this.sslConfiguration = builder.build();
            return this;
        }

        public Builder disableSsl() {
            this.sslConfiguration = null;
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
    }
}
