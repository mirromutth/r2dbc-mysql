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

    /**
     * The new authentication type in MySQL 8.0+.
     */
    String CACHING_SHA2_PASSWORD = "caching_sha2_password";

    /**
     * The most generic authentication type in MySQL 5.x.
     */
    String MYSQL_NATIVE_PASSWORD = "mysql_native_password";

    String SHA256_PASSWORD = "sha256_password";

    /**
     * The Old Password Authentication, it is also the only type of
     * authentication in handshake V9.
     * <p>
     * WARNING: The hashing algorithm has broken that is used for the
     * Old Password Authentication (as shown in CVE-2000-0981).
     */
    String MYSQL_OLD_PASSWORD = "mysql_old_password";

    /**
     * Try use empty string to represent has no authentication provider
     * when {@code Capabilities.PLUGIN_AUTH} does not set.
     */
    String NO_AUTH_PROVIDER = "";

    static MySqlAuthProvider build(String type) {
        requireNonNull(type, "type must not be null");

        switch (type) {
            case CACHING_SHA2_PASSWORD:
                return CachingSha2FastAuthProvider.INSTANCE;
            case MYSQL_NATIVE_PASSWORD:
                return MySqlNativeAuthProvider.INSTANCE;
            case SHA256_PASSWORD:
                return Sha256AuthProvider.INSTANCE;
            case MYSQL_OLD_PASSWORD:
                return OldAuthProvider.INSTANCE;
            case NO_AUTH_PROVIDER:
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
