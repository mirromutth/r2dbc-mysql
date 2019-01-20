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

package io.github.mirromutth.r2dbc.mysql.config;

import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL connection configuration properties
 */
public class ConnectProperties {

    private final boolean useSsl;

    private final String username;

    @Nullable
    private final String password;

    private final String database;

    private final Map<String, String> attributes;

    public ConnectProperties(
        boolean useSsl,
        String username,
        @Nullable String password,
        @Nullable String database,
        @Nullable Map<String, String> attributes
    ) {
        this.useSsl = useSsl;
        this.username = requireNonNull(username, "username must not be null");
        this.password = password;

        if (database == null) {
            this.database = "";
        } else {
            this.database = database;
        }

        if (attributes == null || attributes.isEmpty()) {
            this.attributes = Collections.emptyMap();
        } else if (attributes.size() == 1) {
            Map.Entry<String, String> entry = attributes.entrySet().iterator().next();
            this.attributes = Collections.singletonMap(entry.getKey(), entry.getValue());
        } else {
            this.attributes = new LinkedHashMap<>(attributes);
        }
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    public String getUsername() {
        return username;
    }

    @Nullable
    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
