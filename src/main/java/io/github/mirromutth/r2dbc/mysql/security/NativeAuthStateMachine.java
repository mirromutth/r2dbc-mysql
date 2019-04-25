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

package io.github.mirromutth.r2dbc.mysql.security;

import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import reactor.util.annotation.NonNull;

import java.nio.charset.StandardCharsets;

import static io.github.mirromutth.r2dbc.mysql.constant.AuthType.MYSQL_NATIVE_PASSWORD;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.util.EmptyArrays.EMPTY_BYTES;

/**
 * A MySQL authentication state machine which would use when authentication type is "mysql_native_password".
 */
final class NativeAuthStateMachine implements AuthStateMachine {

    private static final String ALGORITHM = "SHA-1";

    private static final boolean IS_LEFT_SALT = true;

    private static final NativeAuthStateMachine INSTANCE = new NativeAuthStateMachine();

    static NativeAuthStateMachine getInstance() {
        return INSTANCE;
    }

    private NativeAuthStateMachine() {
    }

    /**
     * "mysql_native_password" is not state machine, has no any more data.
     */
    @Override
    public boolean hasNext() {
        return false;
    }

    /**
     * SHA1(password) all bytes xor SHA1( "random data from MySQL server" + SHA1(SHA1(password)) )
     *
     * @param session used to get password and salt.
     * @return encrypted authentication if password is not null, otherwise empty byte array.
     */
    @NonNull
    @Override
    public byte[] nextAuthentication(MySqlSession session) {
        requireNonNull(session, "session must not be null");

        String password = session.getPassword();

        if (password == null || password.isEmpty()) {
            return EMPTY_BYTES;
        }

        byte[] salt = requireNonNull(session.getSalt(), "salt must not be null when password exists");

        return AuthCrypto.usualHash(ALGORITHM, password.getBytes(StandardCharsets.UTF_8), salt, IS_LEFT_SALT);
    }

    @Override
    public String getType() {
        return MYSQL_NATIVE_PASSWORD;
    }
}
