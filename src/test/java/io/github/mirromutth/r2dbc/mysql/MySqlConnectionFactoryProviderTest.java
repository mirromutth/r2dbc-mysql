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
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit test for {@link MySqlConnectionFactoryProvider}.
 */
class MySqlConnectionFactoryProviderTest {

    @Test
    void minimum() {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .build());

        assertEquals(connectionFactory.getClass(), MySqlConnectionFactory.class);
    }

    @Test
    void invalidZeroDate() {
        ConnectionFactory connectionFactory = ConnectionFactories.find(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(Option.valueOf("zeroDate"), "use_nil")
            .build());

        assertNull(connectionFactory);
    }

    @Test
    void validZeroDate() {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(Option.valueOf("zeroDate"), "use_round")
            .build());

        assertEquals(connectionFactory.getClass(), MySqlConnectionFactory.class);
    }
}
