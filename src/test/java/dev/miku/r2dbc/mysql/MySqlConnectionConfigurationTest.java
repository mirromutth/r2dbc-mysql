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
import dev.miku.r2dbc.mysql.extension.Extension;
import io.netty.handler.ssl.SslContextBuilder;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.ThrowableTypeAssert;
import org.junit.jupiter.api.Test;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit tests for {@link MySqlConnectionConfiguration}.
 */
class MySqlConnectionConfigurationTest {

    private static final String HOST = "localhost";

    private static final String UNIX_SOCKET = "/path/to/mysql.sock";

    private static final String USER = "root";

    private static final String SSL_CA = "/path/to/mysql/ca.pem";

    @Test
    void invalid() {
        ThrowableTypeAssert<?> asserted = assertThatIllegalArgumentException();

        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().host(HOST).build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().unixSocket(UNIX_SOCKET).build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().user(USER).build());
    }

    @Test
    void unixSocket() {
        for (SslMode mode : SslMode.values()) {
            if (mode.startSsl()) {
                assertThatIllegalArgumentException().isThrownBy(() -> unixSocketSslMode(mode)).withMessageContaining("sslMode");
            } else {
                assertThat(unixSocketSslMode(SslMode.DISABLED)).isNotNull();
            }
        }

        ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(MySqlConnectionConfiguration.builder()
            .unixSocket(UNIX_SOCKET)
            .user(USER)
            .build());
        asserted.extracting(MySqlConnectionConfiguration::getDomain).isEqualTo(UNIX_SOCKET);
        asserted.extracting(MySqlConnectionConfiguration::getUser).isEqualTo(USER);
        asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(false);
        asserted.extracting(MySqlConnectionConfiguration::getSsl).extracting(MySqlSslConfiguration::getSslMode).isEqualTo(SslMode.DISABLED);
    }

    @Test
    void hosted() {
        ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .build());
        asserted.extracting(MySqlConnectionConfiguration::getDomain).isEqualTo(HOST);
        asserted.extracting(MySqlConnectionConfiguration::getUser).isEqualTo(USER);
        asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(true);
        asserted.extracting(MySqlConnectionConfiguration::getSsl).extracting(MySqlSslConfiguration::getSslMode).isEqualTo(SslMode.PREFERRED);
    }

    @Test
    void invalidPort() {
        ThrowableTypeAssert<?> asserted = assertThatIllegalArgumentException();

        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().port(-1));
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().port(65536));
    }

    @Test
    void allFillUp() {
        assertThat(filledUp()).extracting(MySqlConnectionConfiguration::getSsl).isNotNull();
    }

    @Test
    void isEquals() {
        assertThat(filledUp()).isEqualTo(filledUp()).extracting(Objects::hashCode).isEqualTo(filledUp().hashCode());
    }

    @Test
    void sslContextBuilderCustomizer() {
        String message = "Worked!";
        Function<SslContextBuilder, SslContextBuilder> customizer = ignored -> {
            throw new IllegalStateException(message);
        };
        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .sslMode(SslMode.REQUIRED)
            .sslContextBuilderCustomizer(customizer)
            .build();

        assertThatIllegalStateException()
            .isThrownBy(() -> configuration.getSsl().customizeSslContext(SslContextBuilder.forClient()))
            .withMessage(message);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void invalidSslContextBuilderCustomizer() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> MySqlConnectionConfiguration.builder().sslContextBuilderCustomizer(null))
            .withMessageContaining("sslContextBuilderCustomizer");
    }

    @Test
    void autodetectExtensions() {
        List<Extension> list = new ArrayList<>();
        MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .build()
            .getExtensions()
            .forEach(Extension.class, list::add);
        assertThat(list).isNotEmpty();
    }

    @Test
    void nonAutodetectExtensions() {
        List<Extension> list = new ArrayList<>();
        MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .autodetectExtensions(false)
            .build()
            .getExtensions()
            .forEach(Extension.class, list::add);
        assertThat(list).isEmpty();
    }

    private static MySqlConnectionConfiguration unixSocketSslMode(SslMode sslMode) {
        return MySqlConnectionConfiguration.builder()
            .unixSocket(UNIX_SOCKET)
            .user(USER)
            .sslMode(sslMode)
            .build();
    }

    private static MySqlConnectionConfiguration hostedSslMode(SslMode sslMode, @Nullable String sslCa) {
        return MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .sslMode(sslMode)
            .sslCa(sslCa)
            .build();
    }

    private static MySqlConnectionConfiguration filledUp() {
        return MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .port(3306)
            .password("database-password-in-here")
            .database("r2dbc")
            .tcpKeepAlive(true)
            .tcpNoDelay(true)
            .connectTimeout(Duration.ofSeconds(3))
            .sslMode(SslMode.VERIFY_IDENTITY)
            .sslCa(SSL_CA)
            .sslCert("/path/to/mysql/client-cert.pem")
            .sslKey("/path/to/mysql/client-key.pem")
            .sslKeyPassword("pem-password-in-here")
            .tlsVersion(TlsVersions.TLS1_1, TlsVersions.TLS1_2, TlsVersions.TLS1_3)
            .serverZoneId(ZoneId.systemDefault())
            .zeroDateOption(ZeroDateOption.USE_NULL)
            .sslHostnameVerifier((host, s) -> true)
            .queryCacheSize(128)
            .prepareCacheSize(0)
            .autodetectExtensions(false)
            .build();
    }
}
