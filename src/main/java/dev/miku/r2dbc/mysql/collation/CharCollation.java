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

package dev.miku.r2dbc.mysql.collation;

import dev.miku.r2dbc.mysql.ServerVersion;

import java.nio.charset.Charset;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

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
        // SHIFT-JIS, WINDOWS-932, EUC-JP and eucJP-OPEN will encode '\u00a5' (the sign of
        // Japanese Yen or Chinese Yuan) to '\' (ASCII 92). X-IBM949, X-IBM949C will encode
        // '\u20a9' (the sign of Korean Won) to '\'. They maybe make confuse for text-based
        // parameters. See also escape string in CodecUtils.
        // So, keep it as UTF-8, until some users need some special features.
        return CharCollations.UTF8MB4_GENERAL_CI;
    }
}
