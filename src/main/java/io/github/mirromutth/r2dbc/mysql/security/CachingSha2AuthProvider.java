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

import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;

import static io.github.mirromutth.r2dbc.mysql.internal.EmptyArrays.EMPTY_BYTES;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlAuthProvider} for type "caching_sha2_password".
 */
final class CachingSha2AuthProvider implements MySqlAuthProvider {

    static final String TYPE = "caching_sha2_password";

    static final CachingSha2AuthProvider INSTANCE = new CachingSha2AuthProvider();

    private static final String ALGORITHM = "SHA-256";

    private static final boolean IS_LEFT_SALT = false;

    private CachingSha2AuthProvider() {
    }

    @Override
    public boolean isSslNecessary() {
        return true;
    }

    /**
     * SHA256(password) `all bytes xor` SHA256( SHA256( ~SHA256(password) ) + "random data from MySQL server" )
     * <p>
     * {@inheritDoc}
     */
    @Override
    public byte[] fastAuthPhase(MySqlSession session) {
        requireNonNull(session, "session must not be null");

        return AuthHelper.defaultFastAuthPhase(ALGORITHM, session, IS_LEFT_SALT);
    }

    @Override
    public byte[] fullAuthPhase(MySqlSession session) {
        // TODO: implement full authentication
        return EMPTY_BYTES;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
