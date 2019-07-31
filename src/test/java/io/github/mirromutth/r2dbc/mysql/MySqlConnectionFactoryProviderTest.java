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

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for {@link MySqlConnectionFactoryProvider}.
 */
class MySqlConnectionFactoryProviderTest {

    @Test
    void get() throws UnsupportedEncodingException {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .build();

        assertEquals(ConnectionFactories.get(options).getClass(), MySqlConnectionFactory.class);

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
            .build();

        assertEquals(ConnectionFactories.get(options).getClass(), MySqlConnectionFactory.class);
        assertEquals(ConnectionFactories.get(
            "r2dbcs:mysql://root:123456@127.0.0.1:3306/r2dbc?" +
                "zeroDate=use_round&" +
                "sslMode=verify_identity&" +
                String.format("tlsVersion=%s&", URLEncoder.encode("TLSv1.1,TLSv1.2,TLSv1.3", "UTF-8")) +
                String.format("sslCa=%s&", URLEncoder.encode("/path/to/ca.pem", "UTF-8")) +
                String.format("sslKey=%s&", URLEncoder.encode("/path/to/client-key.pem", "UTF-8")) +
                String.format("sslCert=%s&", URLEncoder.encode("/path/to/client-cert.pem", "UTF-8")) +
                "sslKeyPassword=ssl123456"
        ).getClass(), MySqlConnectionFactory.class);
    }
}
