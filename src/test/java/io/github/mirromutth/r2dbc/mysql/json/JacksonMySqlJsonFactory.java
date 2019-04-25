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

package io.github.mirromutth.r2dbc.mysql.json;

import io.github.mirromutth.r2dbc.mysql.core.ServerVersion;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An implementation of {@link MySqlJsonFactory} for Jackson.
 */
@Component
@ConditionalOnClass(ObjectMapper.class)
@ConditionalOnBean(ObjectMapper.class)
public class JacksonMySqlJsonFactory implements MySqlJsonFactory {

    private final JacksonMySqlJson jacksonMySqlJson;

    public JacksonMySqlJsonFactory(ObjectMapper objectMapper) {
        this.jacksonMySqlJson = new JacksonMySqlJson(objectMapper);
    }

    @Override
    public MySqlJson build(ServerVersion version) throws IllegalArgumentException {
        return jacksonMySqlJson;
    }
}
