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
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

import javax.net.ssl.HostnameVerifier;
import java.time.ZoneId;
import java.util.function.Function;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;

/**
 * An implementation of {@link ConnectionFactoryProvider} for creating {@link MySqlConnectionFactory}s.
 */
public final class MySqlConnectionFactoryProvider implements ConnectionFactoryProvider {

    public static final String MYSQL_DRIVER = "mysql";

    public static final Option<String> UNIX_SOCKET = Option.valueOf("unixSocket");

    public static final Option<Object> SERVER_ZONE_ID = Option.valueOf("serverZoneId");

    /**
     * Option to configure handling when MySQL server returning "zero date" (aka. "0000-00-00 00:00:00")
     */
    public static final Option<Object> ZERO_DATE = Option.valueOf("zeroDate");

    public static final Option<Object> SSL_MODE = Option.valueOf("sslMode");

    /**
     * Option to configure {@link HostnameVerifier}.
     *
     * @since 0.8.2
     */
    public static final Option<Object> SSL_HOSTNAME_VERIFIER = Option.valueOf("sslHostnameVerifier");

    public static final Option<String> TLS_VERSION = Option.valueOf("tlsVersion");

    public static final Option<String> SSL_CA = Option.valueOf("sslCa");

    public static final Option<String> SSL_KEY = Option.valueOf("sslKey");

    public static final Option<CharSequence> SSL_KEY_PASSWORD = Option.sensitiveValueOf("sslKeyPassword");

    public static final Option<String> SSL_CERT = Option.valueOf("sslCert");

    public static final Option<Object> SSL_CONTEXT_BUILDER_CUSTOMIZER = Option.valueOf("sslContextBuilderCustomizer");

    /**
     * Enable TCP KeepAlive.
     *
     * @since 0.8.2
     */
    public static final Option<Object> TCP_KEEPALIVE = Option.valueOf("tcpKeepAlive");

    /**
     * Enable TCP NoDelay.
     *
     * @since 0.8.2
     */
    public static final Option<Object> TCP_NODELAY = Option.valueOf("tcpNoDelay");

    public static final Option<Object> USE_SERVER_PREPARE_STATEMENT = Option.valueOf("useServerPrepareStatement");

    public static final Option<Boolean> AUTODETECT_EXTENSIONS = Option.valueOf("autodetectExtensions");

    @SuppressWarnings("unchecked")
    @Override
    public ConnectionFactory create(ConnectionFactoryOptions options) {
        requireNonNull(options, "connectionFactoryOptions must not be null");

        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder();

        String unixSocket = options.getValue(UNIX_SOCKET);
        String host = options.getValue(HOST);
        if (unixSocket == null) {
            requireNonNull(host, "host must not be null when unixSocket is null");

            builder.host(host);
        } else {
            builder.unixSocket(unixSocket);
        }

        Integer port = options.getValue(PORT);
        if (port != null) {
            builder.port(port);
        }

        Boolean isSsl = options.getValue(SSL);
        if (isSsl != null) {
            builder.sslMode(isSsl ? SslMode.PREFERRED : SslMode.DISABLED);
        }

        Object sslMode = options.getValue(SSL_MODE);
        if (sslMode != null) {
            if (sslMode instanceof SslMode) {
                builder.sslMode((SslMode) sslMode);
            } else if (sslMode instanceof String) {
                builder.sslMode(SslMode.valueOf(((String) sslMode).toUpperCase()));
            } else {
                throw new IllegalArgumentException("sslMode must be SslMode or a string of SslMode");
            }
        }

        String tlsVersion = options.getValue(TLS_VERSION);
        if (tlsVersion != null) {
            builder.tlsVersion(tlsVersion.split(","));
        }

        Object sslHostnameVerifier = options.getValue(SSL_HOSTNAME_VERIFIER);
        if (sslHostnameVerifier != null) {
            if (sslHostnameVerifier instanceof HostnameVerifier) {
                builder.sslHostnameVerifier((HostnameVerifier) sslHostnameVerifier);
            } else if (sslHostnameVerifier instanceof String) {
                builder.sslHostnameVerifier(newInstance((String) sslHostnameVerifier, HostnameVerifier.class));
            } else {
                throw new IllegalArgumentException("sslHostnameVerifier must be HostnameVerifier");
            }
        }

        String sslCert = options.getValue(SSL_CERT);
        String sslKey = options.getValue(SSL_KEY);
        CharSequence sslKeyPassword = options.getValue(SSL_KEY_PASSWORD);
        if (sslKey != null || sslCert != null) {
            require(sslKey != null && sslCert != null, "SSL key and cert must be both null or both non-null");

            builder.sslKeyAndCert(sslCert, sslKey, sslKeyPassword);
        }

        Object sslContextBuilderCustomizer = options.getValue(SSL_CONTEXT_BUILDER_CUSTOMIZER);
        if (sslContextBuilderCustomizer != null) {
            if (sslContextBuilderCustomizer instanceof String) {
                sslContextBuilderCustomizer = newInstance((String) sslContextBuilderCustomizer, Function.class);
            }

            require(sslContextBuilderCustomizer instanceof Function<?, ?>, "sslContextBuilderCustomizer must be Function");

            builder.sslContextBuilderCustomizer((Function<SslContextBuilder, SslContextBuilder>) sslContextBuilderCustomizer);
        }

        Object serverZoneId = options.getValue(SERVER_ZONE_ID);
        if (serverZoneId != null) {
            if (serverZoneId instanceof ZoneId) {
                builder.serverZoneId((ZoneId) serverZoneId);
            } else if (serverZoneId instanceof String) {
                builder.serverZoneId(ZoneId.of((String) serverZoneId));
            } else {
                throw new IllegalArgumentException("serverZoneId must be ZoneId or a string of ZoneId");
            }
        }

        Object tcpKeepAlive = options.getValue(TCP_KEEPALIVE);
        if (tcpKeepAlive != null) {
            // Convert stringify option.
            builder.tcpKeepAlive(tcpKeepAlive instanceof String ? Boolean
                    .parseBoolean(tcpKeepAlive.toString()) : (Boolean) tcpKeepAlive);
        }

        Object tcpNoDelay = options.getValue(TCP_NODELAY);
        if (tcpNoDelay != null) {
            // Convert stringify option.
            builder.tcpNoDelay(tcpNoDelay instanceof String ? Boolean
                    .parseBoolean(tcpNoDelay.toString()) : (Boolean) tcpNoDelay);
        }

        Object zeroDate = options.getValue(ZERO_DATE);
        if (zeroDate != null) {
            if (zeroDate instanceof ZeroDateOption) {
                builder.zeroDateOption((ZeroDateOption) zeroDate);
            } else if (zeroDate instanceof String) {
                builder.zeroDateOption(ZeroDateOption.valueOf(((String) zeroDate).toUpperCase()));
            } else {
                throw new IllegalArgumentException("zeroDate must be ZeroDateOption or a string of ZeroDateOption");
            }
        }

        Object serverPreparing = options.getValue(USE_SERVER_PREPARE_STATEMENT);
        if (serverPreparing != null) {
            // Convert stringify option.
            if (serverPreparing instanceof String) {
                String value = (String) serverPreparing;

                if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                    serverPreparing = Boolean.parseBoolean(value);
                } else {
                    serverPreparing = newInstance(value, Predicate.class);
                }
            }

            if (serverPreparing instanceof Boolean) {
                if ((Boolean) serverPreparing) {
                    builder.useServerPrepareStatement();
                } else {
                    builder.useClientPrepareStatement();
                }
            } else if (serverPreparing instanceof Predicate<?>) {
                builder.useServerPrepareStatement((Predicate<String>) serverPreparing);
            } else {
                throw new IllegalArgumentException("useServerPrepareStatement must be boolean or Predicate");
            }
        }

        Boolean autodetectExtensions = options.getValue(AUTODETECT_EXTENSIONS);

        if (autodetectExtensions != null) {
            builder.autodetectExtensions(autodetectExtensions);
        }

        MySqlConnectionConfiguration configuration = builder.user(options.getRequiredValue(USER))
            .password(options.getValue(PASSWORD))
            .connectTimeout(options.getValue(CONNECT_TIMEOUT))
            .database(options.getValue(DATABASE))
            .sslCa(options.getValue(SSL_CA))
            .build();

        return MySqlConnectionFactory.from(configuration);
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

    private static <T> T newInstance(String className, Class<T> target) {
        try {
            Class<?> type = Class.forName(className);

            if (target.isAssignableFrom(type)) {
                return target.cast(type.getDeclaredConstructor().newInstance());
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot instantiate '" + className + "'", e);
        }

        throw new IllegalArgumentException("Value '" + className + "' must be an instance of " + target.getSimpleName());
    }
}
