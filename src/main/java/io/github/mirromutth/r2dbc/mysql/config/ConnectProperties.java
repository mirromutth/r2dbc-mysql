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

package io.github.mirromutth.r2dbc.mysql.config;

import io.github.mirromutth.r2dbc.mysql.constant.ZeroDateOption;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL connection configuration properties
 */
public class ConnectProperties {

    private final String host;

    private final int port;

    @Nullable
    private final Duration tcpConnectTimeout;

    private final boolean useSsl;

    private final ZeroDateOption zeroDateOption;

    private final String username;

    private final String password;

    private final String database;

    public ConnectProperties(
        String host,
        int port,
        @Nullable Duration tcpConnectTimeout,
        boolean useSsl,
        ZeroDateOption zeroDateOption,
        String username,
        @Nullable String password,
        @Nullable String database
    ) {
        this.host = requireNonNull(host, "host must not be null");
        this.port = port;
        this.tcpConnectTimeout = tcpConnectTimeout;
        this.useSsl = useSsl;
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.username = requireNonNull(username, "username must not be null");
        this.password = password;

        if (database == null || database.isEmpty()) {
            this.database = "";
        } else {
            this.database = database; // or use `database.intern()`?
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Nullable
    public Duration getTcpConnectTimeout() {
        return tcpConnectTimeout;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    public ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public String toString() {
        return "ConnectProperties{" +
            "host='" + host + '\'' +
            ", port=" + port +
            ", tcpConnectTimeout=" + tcpConnectTimeout +
            ", useSsl=" + useSsl +
            ", zeroDateOption=" + zeroDateOption +
            ", database='" + database + '\'' +
            '}';
    }
}
