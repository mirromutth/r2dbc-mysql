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
import dev.miku.r2dbc.mysql.constant.AuthTypes;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import reactor.util.annotation.Nullable;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL authorization provider for connection phase.
 * <p>
 * More information for MySQL authentication type:
 * <p>
 * Connect a MySQL server those test database, execute
 * {@code SELECT * FROM `information_schema`.`PLUGINS` where `plugin_type` = 'AUTHENTICATION'}
 * could see what authentication plugin types those this MySQL server supports.
 */
public interface MySqlAuthProvider {

    static MySqlAuthProvider build(String type) {
        requireNonNull(type, "type must not be null");

        switch (type) {
            case AuthTypes.CACHING_SHA2_PASSWORD:
                return CachingSha2FastAuthProvider.INSTANCE;
            case AuthTypes.MYSQL_NATIVE_PASSWORD:
                return MySqlNativeAuthProvider.INSTANCE;
            case AuthTypes.SHA256_PASSWORD:
                return Sha256AuthProvider.INSTANCE;
            case AuthTypes.MYSQL_OLD_PASSWORD:
                return OldAuthProvider.INSTANCE;
            case AuthTypes.NO_AUTH_PROVIDER:
                return NoAuthProvider.INSTANCE;
        }

        throw new R2dbcPermissionDeniedException(String.format("Authentication type '%s' not supported", type));
    }

    String getType();

    /**
     * @return true if the authentication type should be used on SSL for next authentication.
     */
    boolean isSslNecessary();

    /**
     * Generate an authorization of the current provider.
     *
     * @param password  user password
     * @param salt      password salt for hash algorithm
     * @param collation password character collation
     * @return fast authentication phase must not be null.
     */
    byte[] authentication(@Nullable CharSequence password, @Nullable byte[] salt, CharCollation collation);

    /**
     * @return next authentication provider for same authentication type, or {@code this} if has not next provider.
     */
    MySqlAuthProvider next();
}
