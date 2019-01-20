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

package io.github.mirromutth.r2dbc.mysql.client;

import io.github.mirromutth.r2dbc.mysql.config.ConnectProperties;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test cases for {@link Client}.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
class ClientTest {

    @Autowired
    private DataSourceProperties dataSourceProperties;

    @Test
    void connect() throws URISyntaxException {
        URI uri = new URI(dataSourceProperties.determineUrl().substring("jdbc:".length()));
        assertNotNull(uri.getHost());
        ConnectProperties properties = new ConnectProperties(
            true,
            dataSourceProperties.determineUsername(),
            dataSourceProperties.determinePassword(),
            dataSourceProperties.determineDatabaseName(),
            null
        );
        List<Object> message = Client.connect(uri.getHost(), uri.getPort(), properties)
            .flatMapMany(client -> Flux.<Object>from(client.getConnectionId()).concatWith(client.getServerVersion()).concatWith(client.getServerVersion()).concatWith(client.getConnectionId()).concatWith(client.getServerVersion()))
            .collectList().block();
        System.out.println(message);
    }
}
