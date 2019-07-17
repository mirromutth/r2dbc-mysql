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

package io.github.mirromutth.r2dbc.mysql.authentication;

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.constant.EmptyArrays.EMPTY_BYTES;

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
    public byte[] fastAuthPhase(@Nullable CharSequence password, @Nullable byte[] salt, CharCollation collation) {
        return AuthHelper.generalHash(ALGORITHM, IS_LEFT_SALT, password, salt, collation);
    }

    @Override
    public byte[] fullAuthPhase(@Nullable CharSequence password, CharCollation collation) {
        // TODO: implement full authentication
        return EMPTY_BYTES;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
