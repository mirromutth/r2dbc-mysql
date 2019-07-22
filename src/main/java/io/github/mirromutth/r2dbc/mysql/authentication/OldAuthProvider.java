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

import static io.github.mirromutth.r2dbc.mysql.constant.AuthTypes.MYSQL_OLD_PASSWORD;

/**
 * An implementation of {@link MySqlAuthProvider} for type "mysql_old_password".
 * <p>
 * WARNING: The hashing algorithm has broken that is used for the Old Password Authentication
 * "mysql_old_password" (as shown in CVE-2000-0981).
 */
final class OldAuthProvider implements MySqlAuthProvider {

    static final OldAuthProvider INSTANCE = new OldAuthProvider();

    private OldAuthProvider() {
    }

    @Override
    public String getType() {
        return MYSQL_OLD_PASSWORD;
    }

    @Override
    public boolean isSslNecessary() {
        return false;
    }

    @Override
    public byte[] fastAuthPhase(@Nullable CharSequence password, @Nullable byte[] salt, CharCollation collation) {
        // TODO: implement
        return new byte[0];
    }

    @Override
    public byte[] fullAuthPhase(@Nullable CharSequence password, CharCollation collation) {
        // "mysql_old_password" does not support full authentication.
        return null;
    }
}
