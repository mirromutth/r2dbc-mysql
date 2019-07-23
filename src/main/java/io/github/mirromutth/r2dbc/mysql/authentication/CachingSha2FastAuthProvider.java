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

import static io.github.mirromutth.r2dbc.mysql.constant.AuthTypes.CACHING_SHA2_PASSWORD;

/**
 * An implementation of {@link MySqlAuthProvider} for type "caching_sha2_password" in fast authentication phase.
 */
final class CachingSha2FastAuthProvider implements MySqlAuthProvider {

    static final CachingSha2FastAuthProvider INSTANCE = new CachingSha2FastAuthProvider();

    private static final String ALGORITHM = "SHA-256";

    private static final boolean IS_LEFT_SALT = false;

    private CachingSha2FastAuthProvider() {
    }

    @Override
    public boolean isSslNecessary() {
        // "caching_sha2_password" no need SSL in fast authentication phase.
        return false;
    }

    /**
     * SHA256(password) `all bytes xor` SHA256( SHA256( ~SHA256(password) ) + "random data from MySQL server" )
     * <p>
     * {@inheritDoc}
     */
    @Override
    public byte[] authentication(@Nullable CharSequence password, @Nullable byte[] salt, CharCollation collation) {
        return AuthHelper.generalHash(ALGORITHM, IS_LEFT_SALT, password, salt, collation);
    }

    @Override
    public MySqlAuthProvider next() {
        return CachingSha2FullAuthProvider.INSTANCE;
    }

    @Override
    public String getType() {
        return CACHING_SHA2_PASSWORD;
    }
}
