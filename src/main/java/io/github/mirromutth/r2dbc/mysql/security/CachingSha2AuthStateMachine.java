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
import java.util.Arrays;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNotEmpty;
import static io.github.mirromutth.r2dbc.mysql.util.EmptyArrays.EMPTY_BYTES;

/**
 * A MySQL authentication state machine which would use when authentication type is "caching_sha2_password".
 *
 * <p>
 * {@link AuthState#COMPLETED} means succeeded or failed
 * <ol>
 * <li>initialize status is {@link AuthState#INITIALIZED}</li>
 * <li>after handshake send salt: {@link AuthState#INITIALIZED} -> {@link AuthState#FAST_AUTH}</li>
 * <li>auth more data 0x03: {@link AuthState#FAST_AUTH} -> {@link AuthState#COMPLETED} (success)</li>
 * <li>auth more data 0x04 with secure connection: {@link AuthState#FAST_AUTH} -> {@link AuthState#FULL_AUTH_SECURED}</li>
 * <li>auth more data 0x04 without secure connection: {@link AuthState#FAST_AUTH} -> {@link AuthState#FULL_AUTH_RSA}</li>
 * <li>...need more data to analysis full authentication mode</li>
 * </ol>
 */
final class CachingSha2AuthStateMachine implements AuthStateMachine {

    private static final String ALGORITHM = "SHA-256";

    private static final boolean IS_LEFT_SALT = false;

    private volatile AuthState authState = AuthState.INITIALIZED;

    CachingSha2AuthStateMachine() {
    }

    @Override
    public byte[] nextAuthentication(MySqlSession session) {
        requireNonNull(session, "session must not be null");

        switch (this.authState) {
            case INITIALIZED:
                byte[] result = challenge(session.getPassword(), session.getSalt());
                this.authState = AuthState.FAST_AUTH;
                return result;
            case FAST_AUTH:
                byte[] authMoreData = requireNotEmpty(session.getAuthMoreData(), "authMoreData must not be null or empty when check fast authentication");

                switch (authMoreData[0]) {
                    case 3: // fast auth success
                        this.authState = AuthState.COMPLETED;
                        return null; // have no authentication for next
                    case 4:
                        // TODO: implement full authentication mode, just use password (plaintext or RSA encrypted)

                        break;
                }

                throw new IllegalArgumentException("unknown authentication more data: " + Arrays.toString(authMoreData));
            case FULL_AUTH_SECURED:
                // TODO: implement full authentication mode after sent password which is plaintext with secure connection
                break;
            case FULL_AUTH_RSA:
                // TODO: implement full authentication mode after sent password which has RSA encrypted
                break;
        }

        return null; // authentication is completed
    }

    /**
     * SHA256(password) all bytes xor SHA256( SHA256(SHA256(password)) + "random data from MySQL server" )
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
        INITIALIZED,
        FAST_AUTH,
        FULL_AUTH_SECURED,
        FULL_AUTH_RSA,
        COMPLETED
    }
}
