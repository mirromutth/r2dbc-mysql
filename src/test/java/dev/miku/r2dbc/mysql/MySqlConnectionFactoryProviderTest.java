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
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import io.netty.handler.ssl.SslContextBuilder;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.assertj.core.api.Assert;
import org.junit.jupiter.api.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.function.Function;
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

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
                String.format("tlsVersion=%s&", URLEncoder.encode("TLSv1.3,TLSv1.2,TLSv1.1", "UTF-8")) +
                String.format("sslCa=%s&", URLEncoder.encode("/path/to/ca.pem", "UTF-8")) +
                String.format("sslKey=%s&", URLEncoder.encode("/path/to/client-key.pem", "UTF-8")) +
                String.format("sslCert=%s&", URLEncoder.encode("/path/to/client-cert.pem", "UTF-8")) +
                "sslKeyPassword=ssl123456"
        )).isExactlyInstanceOf(MySqlConnectionFactory.class);
    }

    @Test
    void urlSslModeInUnixSocket() throws UnsupportedEncodingException {
        Assert<?, SslMode> that = assertThat(SslMode.DISABLED);

        MySqlConnectionConfiguration configuration = MySqlConnectionFactoryProvider.setup(
            ConnectionFactoryOptions.parse("r2dbcs:mysql://root@localhost:3306?" +
                "unixSocket=" + URLEncoder.encode("/path/to/mysql.sock", "UTF-8")));

        that.isEqualTo(configuration.getSsl().getSslMode());

        for (SslMode mode : SslMode.values()) {
            configuration = MySqlConnectionFactoryProvider.setup(
                ConnectionFactoryOptions.parse("r2dbc:mysql://root@localhost:3306?" +
                    "unixSocket=" + URLEncoder.encode("/path/to/mysql.sock", "UTF-8") +
                    "&sslMode=" + mode.name().toLowerCase()));

            that.isEqualTo(configuration.getSsl().getSslMode());
        }
    }

    @Test
    void validProgrammaticHost() {
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
            .option(HOST, "127.0.0.1")
            .option(PORT, 3307)
            .option(USER, "root")
            .option(PASSWORD, "123456")
            .option(SSL, true)
            .option(Option.valueOf(CONNECT_TIMEOUT.name()), Duration.ofSeconds(3).toString())
            .option(DATABASE, "r2dbc")
            .option(Option.valueOf("serverZoneId"), "Asia/Tokyo")
            .option(Option.valueOf("useServerPrepareStatement"), AllTruePredicate.class.getName())
            .option(Option.valueOf("zeroDate"), "use_round")
            .option(Option.valueOf("sslMode"), "verify_identity")
            .option(Option.valueOf("tlsVersion"), "TLSv1.3,TLSv1.2")
            .option(Option.valueOf("sslCa"), "/path/to/ca.pem")
            .option(Option.valueOf("sslKey"), "/path/to/client-key.pem")
            .option(Option.valueOf("sslCert"), "/path/to/client-cert.pem")
            .option(Option.valueOf("sslKeyPassword"), "ssl123456")
            .option(Option.valueOf("sslHostnameVerifier"), MyHostnameVerifier.class.getName())
            .option(Option.valueOf("sslContextBuilderCustomizer"), SslCustomizer.class.getName())
            .option(Option.valueOf("tcpKeepAlive"), "true")
            .option(Option.valueOf("tcpNoDelay"), "true")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        MySqlConnectionConfiguration configuration = MySqlConnectionFactoryProvider.setup(options);

        assertThat(configuration.getDomain()).isEqualTo("127.0.0.1");
        assertThat(configuration.isHost()).isTrue();
        assertThat(configuration.getPort()).isEqualTo(3307);
        assertThat(configuration.getUser()).isEqualTo("root");
        assertThat(configuration.getPassword()).isEqualTo("123456");
        assertThat(configuration.getConnectTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(configuration.getDatabase()).isEqualTo("r2dbc");
        assertThat(configuration.getZeroDateOption()).isEqualTo(ZeroDateOption.USE_ROUND);
        assertThat(configuration.isTcpKeepAlive()).isTrue();
        assertThat(configuration.isTcpNoDelay()).isTrue();
        assertThat(configuration.getServerZoneId()).isEqualTo(ZoneId.of("Asia/Tokyo"));
        assertThat(configuration.getPreferPrepareStatement()).isExactlyInstanceOf(AllTruePredicate.class);
        assertThat(configuration.getExtensions()).isEqualTo(Extensions.from(Collections.emptyList(), true));

        assertThat(configuration.getSsl().getSslMode()).isEqualTo(SslMode.VERIFY_IDENTITY);
        assertThat(configuration.getSsl().getTlsVersion()).isEqualTo(new String[] {"TLSv1.3", "TLSv1.2"});
        assertThat(configuration.getSsl().getSslCa()).isEqualTo("/path/to/ca.pem");
        assertThat(configuration.getSsl().getSslKey()).isEqualTo("/path/to/client-key.pem");
        assertThat(configuration.getSsl().getSslCert()).isEqualTo("/path/to/client-cert.pem");
        assertThat(configuration.getSsl().getSslKeyPassword()).isEqualTo("ssl123456");
        assertThat(configuration.getSsl().getSslHostnameVerifier())
            .isExactlyInstanceOf(MyHostnameVerifier.class);
        assertThatExceptionOfType(MockException.class)
            .isThrownBy(() -> configuration.getSsl().customizeSslContext(SslContextBuilder.forClient()));
    }

    @Test
    void invalidProgrammatic() {
        assertThatIllegalStateException().isThrownBy(() -> MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(PORT, 3307)
            .option(USER, "root")
            .option(PASSWORD, "123456")
            .option(SSL, true)
            .option(CONNECT_TIMEOUT, Duration.ofSeconds(3))
            .option(DATABASE, "r2dbc")
            .option(Option.valueOf("zeroDate"), "use_round")
            .option(Option.valueOf("sslMode"), "verify_identity")
            .option(Option.valueOf("tlsVersion"), "TLSv1.3,TLSv1.2")
            .option(Option.valueOf("sslCa"), "/path/to/ca.pem")
            .option(Option.valueOf("sslKey"), "/path/to/client-key.pem")
            .option(Option.valueOf("sslCert"), "/path/to/client-cert.pem")
            .option(Option.valueOf("sslKeyPassword"), "ssl123456")
            .option(Option.valueOf("tcpKeepAlive"), "true")
            .option(Option.valueOf("tcpNoDelay"), "true")
            .build()))
            .withMessageContaining("host");
    }

    @Test
    void validProgrammaticUnixSocket() {
        Assert<?, String> domain = assertThat("/path/to/mysql.sock");
        Assert<?, Boolean> isHost = assertThat(false);
        Assert<?, SslMode> sslMode = assertThat(SslMode.DISABLED);

        MySqlConnectionConfiguration configuration = MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
            .option(USER, "root")
            .option(SSL, true)
            .build());

        domain.isEqualTo(configuration.getDomain());
        isHost.isEqualTo(configuration.isHost());
        sslMode.isEqualTo(configuration.getSsl().getSslMode());

        for (SslMode mode : SslMode.values()) {
            configuration = MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
                .option(USER, "root")
                .option(Option.valueOf("sslMode"), mode.name().toLowerCase())
                .build());

            domain.isEqualTo(configuration.getDomain());
            isHost.isEqualTo(configuration.isHost());
            sslMode.isEqualTo(configuration.getSsl().getSslMode());
        }

        configuration = MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
            .option(HOST, "127.0.0.1")
            .option(PORT, 3307)
            .option(USER, "root")
            .option(PASSWORD, "123456")
            .option(SSL, true)
            .option(Option.valueOf(CONNECT_TIMEOUT.name()), Duration.ofSeconds(3).toString())
            .option(DATABASE, "r2dbc")
            .option(Option.valueOf("serverZoneId"), "Asia/Tokyo")
            .option(Option.valueOf("useServerPrepareStatement"), AllTruePredicate.class.getName())
            .option(Option.valueOf("zeroDate"), "use_round")
            .option(Option.valueOf("sslMode"), "verify_identity")
            .option(Option.valueOf("tlsVersion"), "TLSv1.3,TLSv1.2")
            .option(Option.valueOf("sslCa"), "/path/to/ca.pem")
            .option(Option.valueOf("sslKey"), "/path/to/client-key.pem")
            .option(Option.valueOf("sslCert"), "/path/to/client-cert.pem")
            .option(Option.valueOf("sslKeyPassword"), "ssl123456")
            .option(Option.valueOf("sslHostnameVerifier"), MyHostnameVerifier.class.getName())
            .option(Option.valueOf("sslContextBuilderCustomizer"), SslCustomizer.class.getName())
            .option(Option.valueOf("tcpKeepAlive"), "true")
            .option(Option.valueOf("tcpNoDelay"), "true")
            .build());

        assertThat(configuration.getDomain()).isEqualTo("/path/to/mysql.sock");
        assertThat(configuration.isHost()).isFalse();
        assertThat(configuration.getPort()).isEqualTo(3306);
        assertThat(configuration.getUser()).isEqualTo("root");
        assertThat(configuration.getPassword()).isEqualTo("123456");
        assertThat(configuration.getConnectTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(configuration.getDatabase()).isEqualTo("r2dbc");
        assertThat(configuration.getZeroDateOption()).isEqualTo(ZeroDateOption.USE_ROUND);
        assertThat(configuration.isTcpKeepAlive()).isTrue();
        assertThat(configuration.isTcpNoDelay()).isTrue();
        assertThat(configuration.getServerZoneId()).isEqualTo(ZoneId.of("Asia/Tokyo"));
        assertThat(configuration.getPreferPrepareStatement()).isExactlyInstanceOf(AllTruePredicate.class);
        assertThat(configuration.getExtensions()).isEqualTo(Extensions.from(Collections.emptyList(), true));

        assertThat(configuration.getSsl().getSslMode()).isEqualTo(SslMode.DISABLED);
        assertThat(configuration.getSsl().getTlsVersion()).isEmpty();
        assertThat(configuration.getSsl().getSslCa()).isNull();
        assertThat(configuration.getSsl().getSslKey()).isNull();
        assertThat(configuration.getSsl().getSslCert()).isNull();
        assertThat(configuration.getSsl().getSslKeyPassword()).isNull();
        assertThat(configuration.getSsl().getSslHostnameVerifier()).isNull();
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
        assertThat(sslContextBuilder)
            .isSameAs(configuration.getSsl().customizeSslContext(sslContextBuilder));
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
        assertThatIllegalArgumentException().isThrownBy(() -> ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, NotPredicate.class.getTypeName())
            .build()));

        assertThatIllegalArgumentException().isThrownBy(() -> ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, NotPredicate.class.getPackage() + "NonePredicate")
            .build()));
    }
}

final class MockException extends RuntimeException {

    static final MockException INSTANCE = new MockException();
}

final class SslCustomizer implements Function<SslContextBuilder, SslContextBuilder> {

    @Override
    public SslContextBuilder apply(SslContextBuilder sslContextBuilder) {
        throw MockException.INSTANCE;
    }
}

final class MyHostnameVerifier implements HostnameVerifier {

    @Override
    public boolean verify(String s, SSLSession sslSession) {
        return true;
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
