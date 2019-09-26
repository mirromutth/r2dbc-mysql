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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
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

    public static final String MYSQL_DRIVER = "mysql";

    /**
     * This option indicates special handling when MySQL server returning "zero date" (aka. "0000-00-00 00:00:00")
     */
    public static final Option<String> ZERO_DATE = Option.valueOf("zeroDate");

    public static final Option<String> SSL_MODE = Option.valueOf("sslMode");

    public static final Option<String> TLS_VERSION = Option.valueOf("tlsVersion");

    public static final Option<String> SSL_CA = Option.valueOf("sslCa");

    public static final Option<String> SSL_KEY = Option.valueOf("sslKey");

    public static final Option<CharSequence> SSL_KEY_PASSWORD = Option.sensitiveValueOf("sslKeyPassword");

    public static final Option<String> SSL_CERT = Option.valueOf("sslCert");

    @Override
    public ConnectionFactory create(ConnectionFactoryOptions options) {
        requireNonNull(options, "connectionFactoryOptions must not be null");

        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder();

        String zeroDate = options.getValue(ZERO_DATE);
        if (zeroDate != null) {
            builder.zeroDateOption(ZeroDateOption.valueOf(zeroDate.toUpperCase()));
        }

        Integer port = options.getValue(PORT);
        if (port != null) {
            builder.port(port);
        }

        Boolean isSsl = options.getValue(SSL);
        if (isSsl != null && !isSsl) {
            builder.sslMode(SslMode.DISABLED);
        }

        String sslMode = options.getValue(SSL_MODE);
        if (sslMode != null) {
            builder.sslMode(SslMode.valueOf(sslMode.toUpperCase()));
        }

        String tlsVersion = options.getValue(TLS_VERSION);
        if (tlsVersion != null) {
            builder.tlsVersion(tlsVersion.split(","));
        }

        String sslCert = options.getValue(SSL_CERT);
        String sslKey = options.getValue(SSL_KEY);
        CharSequence sslKeyPassword = options.getValue(SSL_KEY_PASSWORD);
        if (sslKey != null || sslCert != null) {
            require(sslKey != null && sslCert != null, "SSL key and cert must be both null or both non-null");

            builder.sslKeyAndCert(sslCert, sslKey, sslKeyPassword);
        }

        MySqlConnectionConfiguration configuration = builder.host(options.getRequiredValue(HOST))
            .username(options.getRequiredValue(USER))
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
}
