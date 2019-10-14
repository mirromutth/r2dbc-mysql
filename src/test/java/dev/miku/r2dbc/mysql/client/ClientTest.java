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

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.MySqlConnectionConfiguration;
import dev.miku.r2dbc.mysql.MySqlConnectionConfigurationTest;
import dev.miku.r2dbc.mysql.MySqlSslConfiguration;
import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.constant.TlsVersions;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link Client}.
 */
class ClientTest {

    private static final ConnectionProvider PROVIDER = mock(ConnectionProvider.class);

    @Test
    void connectOutOfPortRange() {
        assertThat(assertThrows(IllegalArgumentException.class, connectForPort(-1)).getMessage()).containsIgnoringCase("port");
        assertThat(assertThrows(IllegalArgumentException.class, connectForPort(65536)).getMessage()).containsIgnoringCase("port");
    }

    private static Executable connectForPort(int port) {
        return () -> Client.connect(PROVIDER, "localhost", port, sslConfiguration(), new ConnectionContext(ZeroDateOption.USE_NULL), null);
    }

    private static MySqlSslConfiguration sslConfiguration() {
        return MySqlConnectionConfigurationTest.getSsl(MySqlConnectionConfiguration.builder()
            .host("127.0.0.1")
            .port(3306)
            .username("root")
            .connectTimeout(Duration.ofSeconds(3))
            .zeroDateOption(ZeroDateOption.USE_NULL)
            .sslMode(SslMode.VERIFY_IDENTITY)
            .tlsVersion(TlsVersions.TLS1_3)
            .sslCa("/path/to/mysql/ca.pem")
            .sslKeyAndCert("/path/to/mysql/client-cert.pem", "/path/to/mysql/client-key.pem")
            .build());
    }
}
