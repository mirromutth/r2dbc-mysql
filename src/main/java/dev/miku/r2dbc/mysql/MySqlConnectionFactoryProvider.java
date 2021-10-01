/*
 * Copyright 2018-2021 the original author or authors.
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
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

import javax.net.ssl.HostnameVerifier;
import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Function;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

/**
 * An implementation of {@link ConnectionFactoryProvider} for creating {@link MySqlConnectionFactory}s.
 */
public final class MySqlConnectionFactoryProvider implements ConnectionFactoryProvider {

    /**
     * The name of the driver used for discovery, should not be changed.
     */
    public static final String MYSQL_DRIVER = "mysql";

    /**
     * Option to set the Unix Domain Socket.
     *
     * @since 0.8.1
     */
    public static final Option<String> UNIX_SOCKET = Option.valueOf("unixSocket");

    /**
     * Option to set {@link ZoneId} of server. If it is set, driver will ignore the real time zone of
     * server-side.
     *
     * @since 0.8.2
     */
    public static final Option<ZoneId> SERVER_ZONE_ID = Option.valueOf("serverZoneId");

    /**
     * Option to configure handling when MySQL server returning "zero date" (aka. "0000-00-00 00:00:00")
     *
     * @since 0.8.1
     */
    public static final Option<ZeroDateOption> ZERO_DATE = Option.valueOf("zeroDate");

    /**
     * Option to {@link SslMode}.
     *
     * @since 0.8.1
     */
    public static final Option<SslMode> SSL_MODE = Option.valueOf("sslMode");

    /**
     * Option to configure {@link HostnameVerifier}. It is available only if the {@link #SSL_MODE} set to
     * {@link SslMode#VERIFY_IDENTITY}. It can be an implementation class name of {@link HostnameVerifier}
     * with a public no-args constructor.
     *
     * @since 0.8.2
     */
    public static final Option<HostnameVerifier> SSL_HOSTNAME_VERIFIER =
        Option.valueOf("sslHostnameVerifier");

    /**
     * Option to TLS versions for SslContext protocols, see also {@code TlsVersions}. Usually sorted from
     * higher to lower. It can be a {@code Collection<String>}. It can be a {@link String}, protocols will be
     * split by {@code ,}. e.g. "TLSv1.2,TLSv1.1,TLSv1".
     *
     * @since 0.8.1
     */
    public static final Option<String[]> TLS_VERSION = Option.valueOf("tlsVersion");

    /**
     * Option to set a PEM file of server SSL CA. It will be used to verify server certificates. And it will
     * be used only if {@link #SSL_MODE} set to {@link SslMode#VERIFY_CA} or higher level.
     *
     * @since 0.8.1
     */
    public static final Option<String> SSL_CA = Option.valueOf("sslCa");

    /**
     * Option to set a PEM file of client SSL key.
     *
     * @since 0.8.1
     */
    public static final Option<String> SSL_KEY = Option.valueOf("sslKey");

    /**
     * Option to set a PEM file password of client SSL key. It will be used only if {@link #SSL_KEY} and
     * {@link #SSL_CERT} set.
     *
     * @since 0.8.1
     */
    public static final Option<CharSequence> SSL_KEY_PASSWORD = Option.sensitiveValueOf("sslKeyPassword");

    /**
     * Option to set a PEM file of client SSL cert.
     *
     * @since 0.8.1
     */
    public static final Option<String> SSL_CERT = Option.valueOf("sslCert");

    /**
     * Option to custom {@link SslContextBuilder}. It can be an implementation class name of {@link Function}
     * with a public no-args constructor.
     *
     * @since 0.8.2
     */
    public static final Option<Function<SslContextBuilder, SslContextBuilder>>
        SSL_CONTEXT_BUILDER_CUSTOMIZER = Option.valueOf("sslContextBuilderCustomizer");

    /**
     * TCP socket timeout
     *
     * @since 0.8.3
     */
    public static final Option<Duration> SOCKET_TIMEOUT = Option.valueOf("socketTimeout");

    /**
     * Enable/Disable TCP KeepAlive.
     *
     * @since 0.8.2
     */
    public static final Option<Boolean> TCP_KEEP_ALIVE = Option.valueOf("tcpKeepAlive");

    /**
     * Enable/Disable TCP NoDelay.
     *
     * @since 0.8.2
     */
    public static final Option<Boolean> TCP_NO_DELAY = Option.valueOf("tcpNoDelay");

    /**
     * Enable server preparing for parametrized statements and prefer server preparing simple statements.
     * <p>
     * The value can be a {@link Boolean}. If it is {@code true}, driver will use server preparing for
     * parametrized statements and text query for simple statements. If it is {@code false}, driver will use
     * client preparing for parametrized statements and text query for simple statements.
     * <p>
     * The value can be a {@link Predicate}{@code <}{@link String}{@code >}. If it is set, driver will server
     * preparing for parametrized statements, it configures whether to prefer prepare execution on a
     * statement-by-statement basis (simple statements). The {@link Predicate}{@code <}{@link String}{@code >}
     * accepts the simple SQL query string and returns a {@code boolean} flag indicating preference.
     * <p>
     * The value can be a {@link String}. If it is set, driver will try to convert it to {@link Boolean} or an
     * instance of {@link Predicate}{@code <}{@link String}{@code >} which use reflection with a public
     * no-args constructor.
     *
     * @since 0.8.1
     */
    public static final Option<Object> USE_SERVER_PREPARE_STATEMENT =
        Option.valueOf("useServerPrepareStatement");

    /**
     * Option to set the maximum size of the {@link Query} parsing cache.  Default to {@code 256}.
     *
     * @since 0.8.3
     */
    public static final Option<Integer> PREPARE_CACHE_SIZE = Option.valueOf("prepareCacheSize");

    /**
     * Option to set the maximum size of the server-preparing cache.  Default to {@code 0}.
     *
     * @since 0.8.3
     */
    public static final Option<Integer> QUERY_CACHE_SIZE = Option.valueOf("queryCacheSize");

    /**
     * Enable/Disable auto-detect driver extensions.
     *
     * @since 0.8.2
     */
    public static final Option<Boolean> AUTODETECT_EXTENSIONS = Option.valueOf("autodetectExtensions");

    @Override
    public ConnectionFactory create(ConnectionFactoryOptions options) {
        requireNonNull(options, "connectionFactoryOptions must not be null");

        return MySqlConnectionFactory.from(setup(options));
    }

    @Override
    public boolean supports(ConnectionFactoryOptions options) {
        requireNonNull(options, "connectionFactoryOptions must not be null");
        return MYSQL_DRIVER.equals(options.getValue(DRIVER));
    }

    @Override
    public String getDriver() {
        return MYSQL_DRIVER;
    }

    /**
     * Visible for unit tests.
     *
     * @param options the {@link ConnectionFactoryOptions} for setup {@link MySqlConnectionConfiguration}.
     * @return completed {@link MySqlConnectionConfiguration}.
     */
    static MySqlConnectionConfiguration setup(ConnectionFactoryOptions options) {
        OptionMapper mapper = new OptionMapper(options);
        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder();

        mapper.requires(USER).asString()
            .to(builder::user);
        mapper.optional(PASSWORD).asPassword()
            .to(builder::password);
        mapper.optional(UNIX_SOCKET).asString()
            .to(builder::unixSocket)
            .otherwise(() -> setupHost(builder, mapper));
        mapper.optional(SERVER_ZONE_ID).as(ZoneId.class, id -> ZoneId.of(id, ZoneId.SHORT_IDS))
            .to(builder::serverZoneId);
        mapper.optional(TCP_KEEP_ALIVE).asBoolean()
            .to(builder::tcpKeepAlive);
        mapper.optional(TCP_NO_DELAY).asBoolean()
            .to(builder::tcpNoDelay);
        mapper.optional(ZERO_DATE)
            .as(ZeroDateOption.class, id -> ZeroDateOption.valueOf(id.toUpperCase()))
            .to(builder::zeroDateOption);
        mapper.optional(USE_SERVER_PREPARE_STATEMENT).prepare(builder::useClientPrepareStatement,
            builder::useServerPrepareStatement, builder::useServerPrepareStatement);
        mapper.optional(QUERY_CACHE_SIZE).asInt()
            .to(builder::queryCacheSize);
        mapper.optional(PREPARE_CACHE_SIZE).asInt()
            .to(builder::prepareCacheSize);
        mapper.optional(AUTODETECT_EXTENSIONS).asBoolean()
            .to(builder::autodetectExtensions);
        mapper.optional(CONNECT_TIMEOUT).as(Duration.class, Duration::parse)
            .to(builder::connectTimeout);
        mapper.optional(SOCKET_TIMEOUT).as(Duration.class, Duration::parse)
            .to(builder::socketTimeout);
        mapper.optional(DATABASE).asString()
            .to(builder::database);

        return builder.build();
    }

    /**
     * Set builder of {@link MySqlConnectionConfiguration} for hostname-based address with SSL
     * configurations.
     *
     * @param builder the builder of {@link MySqlConnectionConfiguration}.
     * @param mapper  the {@link OptionMapper} of {@code options}.
     */
    @SuppressWarnings("unchecked")
    private static void setupHost(MySqlConnectionConfiguration.Builder builder, OptionMapper mapper) {
        mapper.requires(HOST).asString()
            .to(builder::host);
        mapper.optional(PORT).asInt()
            .to(builder::port);
        mapper.optional(SSL).asBoolean()
            .to(isSsl -> builder.sslMode(isSsl ? SslMode.REQUIRED : SslMode.DISABLED));
        mapper.optional(SSL_MODE).as(SslMode.class, id -> SslMode.valueOf(id.toUpperCase()))
            .to(builder::sslMode);
        mapper.optional(TLS_VERSION).asStrings()
            .to(builder::tlsVersion);
        mapper.optional(SSL_HOSTNAME_VERIFIER).as(HostnameVerifier.class)
            .to(builder::sslHostnameVerifier);
        mapper.optional(SSL_CERT).asString()
            .to(builder::sslCert);
        mapper.optional(SSL_KEY).asString()
            .to(builder::sslKey);
        mapper.optional(SSL_KEY_PASSWORD).asPassword()
            .to(builder::sslKeyPassword);
        mapper.optional(SSL_CONTEXT_BUILDER_CUSTOMIZER).as(Function.class)
            .to(builder::sslContextBuilderCustomizer);
        mapper.optional(SSL_CA).asString()
            .to(builder::sslCa);
    }
}
