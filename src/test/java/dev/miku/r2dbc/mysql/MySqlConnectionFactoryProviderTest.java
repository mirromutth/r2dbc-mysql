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
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.MySqlConnectionFactoryProvider.USE_SERVER_PREPARE_STATEMENT;
import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test for {@link MySqlConnectionFactoryProvider}.
 */
class MySqlConnectionFactoryProviderTest {

    @Test
    void validUrl() throws UnsupportedEncodingException {
        assertThat(ConnectionFactories.get("r2dbc:mysql://root@localhost:3306")).isExactlyInstanceOf(MySqlConnectionFactory.class);
        assertThat(ConnectionFactories.get("r2dbcs:mysql://root@localhost:3306")).isExactlyInstanceOf(MySqlConnectionFactory.class);
        assertThat(ConnectionFactories.get("r2dbc:mysql://root@localhost:3306?unixSocket=" + URLEncoder.encode("/path/to/mysql.sock", "UTF-8")))
            .isExactlyInstanceOf(MySqlConnectionFactory.class);

        assertThat(ConnectionFactories.get("r2dbcs:mysql://root@localhost:3306?" +
            "unixSocket=" + URLEncoder.encode("/path/to/mysql.sock", "UTF-8") +
            "&sslMode=disabled")).isNotNull();

        assertThat(ConnectionFactories.get(
            "r2dbcs:mysql://root:123456@127.0.0.1:3306/r2dbc?" +
                "zeroDate=use_round&" +
                "sslMode=verify_identity&" +
                "serverPreparing=true" +
                String.format("tlsVersion=%s&", URLEncoder.encode("TLSv1.1,TLSv1.2,TLSv1.3", "UTF-8")) +
                String.format("sslCa=%s&", URLEncoder.encode("/path/to/ca.pem", "UTF-8")) +
                String.format("sslKey=%s&", URLEncoder.encode("/path/to/client-key.pem", "UTF-8")) +
                String.format("sslCert=%s&", URLEncoder.encode("/path/to/client-cert.pem", "UTF-8")) +
                "sslKeyPassword=ssl123456"
        )).isExactlyInstanceOf(MySqlConnectionFactory.class);
    }

    @Test
    void invalidUrl() {
        assertThat(assertThrows(
            IllegalArgumentException.class,
            () -> ConnectionFactories.get("r2dbcs:mysql://root@localhost:3306?" +
                "unixSocket=" + URLEncoder.encode("/path/to/mysql.sock", "UTF-8"))).getMessage())
            .contains("sslMode");

        for (SslMode mode : SslMode.values()) {
            if (mode.startSsl()) {
                assertThat(assertThrows(
                    IllegalArgumentException.class,
                    () -> ConnectionFactories.get("r2dbc:mysql://root@localhost:3306?" +
                        "unixSocket=" + URLEncoder.encode("/path/to/mysql.sock", "UTF-8") +
                        "&sslMode=" + mode.name().toLowerCase())).getMessage())
                    .contains("sslMode");
            }
        }
    }

    @Test
    void validProgrammatic() {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(SSL, true)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
            .option(USER, "root")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
            .option(USER, "root")
            .option(Option.valueOf("sslMode"), "disabled")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
            .option(USER, "root")
            .option(SSL, false)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
            .option(USER, "root")
            .option(Option.valueOf("sslMode"), "disabled")
            .option(SSL, true)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(PORT, 3307)
            .option(USER, "root")
            .option(PASSWORD, "123456")
            .option(SSL, true)
            .option(CONNECT_TIMEOUT, Duration.ofSeconds(3))
            .option(DATABASE, "r2dbc")
            .option(Option.valueOf("zeroDate"), "use_round")
            .option(Option.valueOf("sslMode"), "verify_identity")
            .option(Option.valueOf("tlsVersion"), "TLSv1.2,TLSv1.3")
            .option(Option.valueOf("sslCa"), "/path/to/ca.pem")
            .option(Option.valueOf("sslKey"), "/path/to/client-key.pem")
            .option(Option.valueOf("sslCert"), "/path/to/client-cert.pem")
            .option(Option.valueOf("sslKeyPassword"), "ssl123456")
            .option(Option.valueOf("tcpKeepAlive"), "true")
            .option(Option.valueOf("tcpNoDelay"), "true")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);
    }

    @Test
    void invalidProgrammatic() {
        assertThat(assertThrows(IllegalArgumentException.class, () -> ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
            .option(USER, "root")
            .option(SSL, true)
            .build()))
            .getMessage())
            .contains("sslMode");

        for (SslMode mode : SslMode.values()) {
            if (mode.startSsl()) {
                assertThat(assertThrows(IllegalArgumentException.class, () -> ConnectionFactories.get(ConnectionFactoryOptions.builder()
                    .option(DRIVER, "mysql")
                    .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
                    .option(USER, "root")
                    .option(Option.valueOf("sslMode"), mode.name().toLowerCase())
                    .build()))
                    .getMessage())
                    .contains("sslMode");
            }
        }
    }

    @Test
    void serverPreparing() {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, AllTruePredicate.class.getTypeName())
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, "true")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, "false")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, AllTruePredicate.INSTANCE)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, true)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, false)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);
    }

    @Test
    void invalidServerPreparing() {
        assertThrows(IllegalArgumentException.class, () -> ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, NotPredicate.class.getTypeName())
            .build()));

        assertThrows(IllegalArgumentException.class, () -> ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, NotPredicate.class.getPackage() + "NonePredicate")
            .build()));
    }
}

final class AllTruePredicate implements Predicate<String> {

    static final Predicate<String> INSTANCE = new AllTruePredicate();

    @Override
    public boolean test(String s) {
        return true;
    }
}

final class NotPredicate {

    public boolean test(String s) {
        return s.length() > 0;
    }
}
