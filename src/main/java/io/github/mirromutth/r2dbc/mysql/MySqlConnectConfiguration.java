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

import io.github.mirromutth.r2dbc.mysql.constant.ZeroDate;
import reactor.util.annotation.Nullable;

import java.time.Duration;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL configuration of connect.
 */
public final class MySqlConnectConfiguration {

    /**
     * Default MySQL port.
     */
    private static final int DEFAULT_PORT = 3306;

    private final String host;

    private final int port;

    @Nullable
    private final Duration connectTimeout;

    private final boolean useSsl;

    private final ZeroDate zeroDate;

    private final String username;

    private final String password;

    private final String database;

    private MySqlConnectConfiguration(
        String host,
        int port,
        @Nullable Duration connectTimeout,
        boolean useSsl,
        ZeroDate zeroDate,
        String username,
        @Nullable String password,
        @Nullable String database
    ) {
        this.host = requireNonNull(host, "host must not be null");
        this.port = port;
        this.connectTimeout = connectTimeout;
        this.useSsl = useSsl;
        this.zeroDate = requireNonNull(zeroDate, "zeroDate must not be null");
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

    boolean isUseSsl() {
        return useSsl;
    }

    ZeroDate getZeroDate() {
        return zeroDate;
    }

    String getUsername() {
        return username;
    }

    String getPassword() {
        return password;
    }

    String getDatabase() {
        return database;
    }

    @Override
    public String toString() {
        return "MySqlConnectConfiguration{" +
            "host='" + host + '\'' +
            ", port=" + port +
            ", connectTimeout=" + connectTimeout +
            ", useSsl=" + useSsl +
            ", zeroDate=" + zeroDate +
            ", username=<hidden>" +
            ", password=<hidden>" +
            ", database='" + database + '\'' +
            '}';
    }

    public static final class Builder {

        @Nullable
        private String database;

        private String host;

        @Nullable
        private String password;

        private int port = DEFAULT_PORT;

        @Nullable
        private Duration connectTimeout;

        private boolean useSsl;

        private String username;

        private ZeroDate zeroDate = ZeroDate.USE_NULL;

        private Builder() {
        }

        public MySqlConnectConfiguration build() {
            return new MySqlConnectConfiguration(host, port, connectTimeout, useSsl, zeroDate, username, password, database);
        }

        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        public Builder host(String host) {
            this.host = requireNonNull(host, "host must not be null");
            return this;
        }

        public Builder password(@Nullable String password) {
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

        public Builder enableSsl() {
            this.useSsl = true;
            return this;
        }

        public Builder disableSsl() {
            this.useSsl = false;
            return this;
        }

        public Builder username(String username) {
            this.username = requireNonNull(username, "username must not be null");
            return this;
        }

        public Builder zeroDate(ZeroDate zeroDate) {
            this.zeroDate = requireNonNull(zeroDate, "zeroDate must not be null");
            return this;
        }
    }
}
