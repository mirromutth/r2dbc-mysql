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

import io.r2dbc.spi.ConnectionMetadata;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * Connection metadata for a connection connected to MySQL database.
 */
public final class MySqlConnectionMetadata implements ConnectionMetadata {

    private final String version;

    private final String product;

    MySqlConnectionMetadata(String version, @Nullable String product) {
        this.version = requireNonNull(version, "version must not be null");
        this.product = product == null ? "Unknown" : product;
    }

    @Override
    public String getDatabaseVersion() {
        return version;
    }

    @Override
    public String getDatabaseProductName() {
        return product;
    }
}
