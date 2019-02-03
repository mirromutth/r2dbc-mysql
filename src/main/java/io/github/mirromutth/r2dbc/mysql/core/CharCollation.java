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

package io.github.mirromutth.r2dbc.mysql.core;

import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Character collation of MySQL
 */
public final class CharCollation {

    private final int id;

    private final String name;

    private final Charset charset;

    public CharCollation(int id, String name, Charset charset) {
        this.id = id;
        this.name = requireNonNull(name, "name must not be null");
        this.charset = requireNonNull(charset, "charset must not be null");
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Charset getCharset() {
        return charset;
    }

    public static CharCollation fromId(int id) {
        return CharCollations.getInstance().fromId(id);
    }
}
