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

import io.github.mirromutth.r2dbc.mysql.core.ServerVersion;

import java.nio.charset.Charset;

/**
 * Character collation of MySQL
 */
public interface CharCollation {

    int getId();

    String getName();

    int getByteSize();

    Charset getCharset();

    static CharCollation defaultCollation(ServerVersion version) {
        return CharCollations.getInstance().defaultCollation(version);
    }

    static CharCollation fromId(int id, ServerVersion version) {
        CharCollation result = CharCollations.getInstance().fromId(id);

        if (result == null) {
            return defaultCollation(version);
        }

        return result;
    }
}
