/*
 * Copyright 2018-2020 the original author or authors.
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
import dev.miku.r2dbc.mysql.constant.TlsVersions;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link MySqlConnectionConfiguration}.
 */
class MySqlConnectionConfigurationTest {

    private static final String HOST = "localhost";

    private static final String UNIX_SOCKET = "/path/to/mysql.sock";

    private static final String USERNAME = "root";

    private static final String SSL_CA = "/path/to/mysql/ca.pem";

    @Test
    void invalid() {
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().build());
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().host(HOST).build());
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().unixSocket(UNIX_SOCKET).build());
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().username(USERNAME).build());
    }

    @Test
    void unixSocket() {
        for (SslMode mode : SslMode.values()) {
            if (mode.startSsl()) {
                assertThat(assertThrows(IllegalArgumentException.class, () -> unixSocketSslMode(mode)).getMessage()).contains("sslMode");
            } else {
                assertThat(unixSocketSslMode(SslMode.DISABLED)).isNotNull();
            }
        }

        ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(MySqlConnectionConfiguration.builder()
            .unixSocket(UNIX_SOCKET)
            .username(USERNAME)
            .build());
        asserted.extracting(MySqlConnectionConfiguration::getDomain).isEqualTo(UNIX_SOCKET);
        asserted.extracting(MySqlConnectionConfiguration::getUsername).isEqualTo(USERNAME);
        asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(false);
        asserted.extracting(MySqlConnectionConfiguration::getSsl).extracting(MySqlSslConfiguration::getSslMode).isEqualTo(SslMode.DISABLED);
    }

    @Test
    void hosted() {
        for (SslMode mode : SslMode.values()) {
            if (mode.verifyCertificate()) {
                assertThat(assertThrows(IllegalArgumentException.class, () -> hostedSslMode(mode, null)).getMessage())
                    .contains("sslCa");
                assertThat(hostedSslMode(mode, SSL_CA)).isNotNull();
            } else {
                assertThat(hostedSslMode(mode, null)).isNotNull();
            }
        }

        ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(MySqlConnectionConfiguration.builder()
            .host(HOST)
            .username(USERNAME)
            .build());
        asserted.extracting(MySqlConnectionConfiguration::getDomain).isEqualTo(HOST);
        asserted.extracting(MySqlConnectionConfiguration::getUsername).isEqualTo(USERNAME);
        asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(true);
        asserted.extracting(MySqlConnectionConfiguration::getSsl).extracting(MySqlSslConfiguration::getSslMode).isEqualTo(SslMode.PREFERRED);
    }

    @Test
    void outOfPortRange() {
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().port(-1));
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().port(65536));
    }

    @Test
    void allFillUp() {
        assertThat(filledUp()).extracting(MySqlConnectionConfiguration::getSsl).isNotNull();
    }

    @Test
    void isEquals() {
        assertThat(filledUp()).isEqualTo(filledUp()).extracting(Objects::hashCode).isEqualTo(filledUp().hashCode());
    }

    private static MySqlConnectionConfiguration unixSocketSslMode(SslMode sslMode) {
        return MySqlConnectionConfiguration.builder()
            .unixSocket(UNIX_SOCKET)
            .username(USERNAME)
            .sslMode(sslMode)
            .build();
    }

    private static MySqlConnectionConfiguration hostedSslMode(SslMode sslMode, @Nullable String sslCa) {
        return MySqlConnectionConfiguration.builder()
            .host(HOST)
            .username(USERNAME)
            .sslMode(sslMode)
            .sslCa(sslCa)
            .build();
    }

    private static MySqlConnectionConfiguration filledUp() {
        return MySqlConnectionConfiguration.builder()
            .host(HOST)
            .username(USERNAME)
            .port(3306)
            .password("database-password-in-here")
            .database("r2dbc")
            .connectTimeout(Duration.ofSeconds(3))
            .sslMode(SslMode.VERIFY_IDENTITY)
            .sslCa(SSL_CA)
            .sslKeyAndCert("/path/to/mysql/client-cert.pem", "/path/to/mysql/client-key.pem", "pem-password-in-here")
            .tlsVersion(TlsVersions.TLS1_1, TlsVersions.TLS1_2, TlsVersions.TLS1_3)
            .zeroDateOption(ZeroDateOption.USE_NULL)
            .build();
    }
}
