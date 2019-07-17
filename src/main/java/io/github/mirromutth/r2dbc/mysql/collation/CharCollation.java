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

package io.github.mirromutth.r2dbc.mysql.collation;

import io.github.mirromutth.r2dbc.mysql.ServerVersion;

import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * Character collation of MySQL
 */
public interface CharCollation {

    int getId();

    String getName();

    int getByteSize();

    Charset getCharset();

    int BINARY_ID = CharCollations.BINARY.getId();

    static CharCollation fromId(int id, ServerVersion version) {
        requireNonNull(version, "version must not be null");

        return CharCollations.fromId(id, version);
    }

    /**
     * WARNING: this method is internal method in `r2dbc-mysql`, it is UNSTABLE and may change.
     *
     * @return client character collation.
     */
    static CharCollation clientCharCollation() {
        return CharCollations.UTF8MB4_GENERAL_CI;
    }
}
