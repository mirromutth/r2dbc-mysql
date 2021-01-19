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

package dev.miku.r2dbc.mysql.authentication;

import dev.miku.r2dbc.mysql.collation.CharCollation;
import reactor.util.annotation.Nullable;

import java.nio.CharBuffer;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlAuthProvider} for type "sha256_password".
 */
final class Sha256AuthProvider implements MySqlAuthProvider {

    static final Sha256AuthProvider INSTANCE = new Sha256AuthProvider();

    @Override
    public boolean isSslNecessary() {
        return true;
    }

    @Override
    public byte[] authentication(@Nullable CharSequence password, byte[] salt, CharCollation collation) {
        if (password == null || password.length() <= 0) {
            return new byte[] { TERMINAL };
        }

        requireNonNull(collation, "collation must not be null when password exists");

        return AuthUtils.encodeTerminal(CharBuffer.wrap(password), collation.getCharset());
    }

    @Override
    public MySqlAuthProvider next() {
        return this;
    }

    @Override
    public String getType() {
        return SHA256_PASSWORD;
    }

    private Sha256AuthProvider() { }
}
