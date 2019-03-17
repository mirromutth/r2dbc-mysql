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
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.util.EmptyArrays.EMPTY_BYTES;

/**
 * A MySQL authentication state machine which would use when authentication type is "caching_sha2_password".
 */
final class CachingSha2AuthStateMachine implements AuthStateMachine {

    private static final String ALGORITHM = "SHA-256";

    private static final boolean IS_LEFT_SALT = false;

    private final AtomicReference<AuthState> authState = new AtomicReference<>(AuthState.FAST_AUTH);

    CachingSha2AuthStateMachine() {
    }

    @Override
    public boolean hasNext() {
        return this.authState.get() != AuthState.COMPLETED;
    }

    @Override
    public byte[] nextAuthentication(MySqlSession session) {
        requireNonNull(session, "session must not be null");

        while (true) {
            AuthState authState = this.authState.get();
            if (this.authState.compareAndSet(authState, authState.next())) {
                switch (authState) {
                    case FAST_AUTH:
                        return challenge(session.getPassword(), session.getSalt());
                    case FULL_AUTH:
                        // TODO: implement full authentication mode
                        break;
                }

                break;
            }
        }

        return null; // authentication is completed
    }

    /**
     * SHA256(password) all bytes xor SHA256( SHA256(   ~SHA256(password)) + "random data from MySQL server" )
     *
     * @param password user password
     * @param salt     random data from MySQL server, it could be null if password is null or empty
     * @return encrypted authentication if password is not null, otherwise empty byte array.
     */
    private byte[] challenge(@Nullable String password, @Nullable byte[] salt) {
        if (password == null || password.isEmpty()) {
            return EMPTY_BYTES;
        }

        requireNonNull(salt, "salt must not be null when password exists");

        return AuthCrypto.usualHash(ALGORITHM, password.getBytes(StandardCharsets.UTF_8), salt, IS_LEFT_SALT);
    }

    private enum AuthState {
        FAST_AUTH {

            @Override
            AuthState next() {
                return AuthState.FULL_AUTH;
            }
        },
        FULL_AUTH {

            @Override
            AuthState next() {
                return AuthState.COMPLETED;
            }
        },
        COMPLETED {

            @Override
            AuthState next() {
                return AuthState.COMPLETED;
            }
        };

        abstract AuthState next();
    }
}
