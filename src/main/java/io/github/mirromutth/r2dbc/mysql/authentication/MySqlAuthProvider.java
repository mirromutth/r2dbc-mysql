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
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL authorization provider for connection phase.
 * <p>
 * Old Password Authentication ("mysql_old_password") will NEVER support, the hashing algorithm
 * has broken that is used for this authentication type (as shown in CVE-2000-0981).
 * <p>
 * More information for MySQL authentication type:
 * <p>
 * Connect a MySQL server those test database, execute
 * {@code SELECT * FROM `information_schema`.`PLUGINS` where `plugin_type` = 'AUTHENTICATION'}
 * could see what authentication plugin types those this MySQL server supports.
 */
public interface MySqlAuthProvider {

    static String defaultAuthType() {
        return MySqlNativeAuthProvider.TYPE;
    }

    static MySqlAuthProvider build(String type) {
        requireNonNull(type, "type must not be null");

        switch (type) {
            case CachingSha2AuthProvider.TYPE:
                return CachingSha2AuthProvider.INSTANCE;
            case MySqlNativeAuthProvider.TYPE:
                return MySqlNativeAuthProvider.INSTANCE;
            case Sha256AuthProvider.TYPE:
                return Sha256AuthProvider.INSTANCE;
        }

        throw new R2dbcPermissionDeniedException("Authentication type '" + type + "' not supported");
    }

    String getType();

    /**
     * @return true if the authentication type should be used on SSL.
     */
    boolean isSslNecessary();

    /**
     * Generate an authorization for fast authentication phase.
     *
     * @param password  user password
     * @param salt      password salt for hash algorithm
     * @param collation password character collation
     * @return fast authentication phase must not be null.
     */
    byte[] fastAuthPhase(@Nullable CharSequence password, @Nullable byte[] salt, CharCollation collation);

    /**
     * Generate an authorization for full authentication phase.
     *
     * @param password  user password
     * @param collation password character collation
     * @return {@code null} means has no full authentication phase.
     */
    @Nullable
    byte[] fullAuthPhase(@Nullable CharSequence password, CharCollation collation);
}
