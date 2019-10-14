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
import dev.miku.r2dbc.mysql.constant.TlsVersions;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link MySqlConnectionConfiguration}.
 */
public class MySqlConnectionConfigurationTest {

    @Test
    void minimum() {
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().build());
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().host("localhost").build());
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().username("root").build());

        assertNotNull(MySqlConnectionConfiguration.builder().host("localhost").username("root").build());
    }

    @Test
    void outOfPortRange() {
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().port(-1));
        assertThrows(IllegalArgumentException.class, () -> MySqlConnectionConfiguration.builder().port(65536));
    }

    @Test
    void allFillUp() {
        MySqlConnectionConfiguration configuration = filledUp();

        assertNotNull(configuration);
        assertNotNull(getSsl(configuration));
    }

    @Test
    void isEquals() {
        assertEquals(filledUp(), filledUp());
    }

    @Test
    void hashCodeEquals() {
        assertEquals(filledUp().hashCode(), filledUp().hashCode());
    }

    public static MySqlSslConfiguration getSsl(MySqlConnectionConfiguration configuration) {
        return configuration.getSsl();
    }

    private static MySqlConnectionConfiguration filledUp() {
        return MySqlConnectionConfiguration.builder()
            .host("127.0.0.1")
            .username("root")
            .port(3306)
            .password("database-password-in-here")
            .database("r2dbc")
            .connectTimeout(Duration.ofSeconds(3))
            .sslMode(SslMode.VERIFY_IDENTITY)
            .sslCa("/path/to/mysql/ca.pem")
            .sslKeyAndCert("/path/to/mysql/client-cert.pem", "/path/to/mysql/client-key.pem", "pem-password-in-here")
            .tlsVersion(TlsVersions.TLS1_1, TlsVersions.TLS1_2, TlsVersions.TLS1_3)
            .zeroDateOption(ZeroDateOption.USE_NULL)
            .build();
    }
}
