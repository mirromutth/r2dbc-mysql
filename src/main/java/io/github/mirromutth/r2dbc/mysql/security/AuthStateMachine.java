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
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.constant.AuthTypes.CACHING_SHA2_PASSWORD;
import static io.github.mirromutth.r2dbc.mysql.constant.AuthTypes.MYSQL_NATIVE_PASSWORD;
import static io.github.mirromutth.r2dbc.mysql.constant.AuthTypes.SHA256_PASSWORD;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL authentication state machine for connection phase.
 */
public interface AuthStateMachine {

    static AuthStateMachine build(String type) {
        switch (requireNonNull(type, "type must not be null")) {
            case CACHING_SHA2_PASSWORD:
                return new CachingSha2AuthStateMachine();
            case MYSQL_NATIVE_PASSWORD:
                return NativeAuthStateMachine.getInstance();
            case SHA256_PASSWORD:
                return Sha256AuthStateMachine.getInstance();
        }

        throw new R2dbcPermissionDeniedException("Authentication type '" + type + "' not supported");
    }

    String getType();

    /**
     * @return true if {@code this} is state machine and it also has next authentication.
     */
    boolean hasNext();

    /**
     * @return true if the authentication type should be used on SSL.
     */
    boolean isSslNecessary();

    /**
     * Generate next authentication and make changes to the authentication status.
     *
     * @param session current MySQL session.
     * @return {@code null} if have no next authentication used to send, and
     * empty means send next authentication which is empty.
     */
    @Nullable
    byte[] nextAuthentication(MySqlSession session);
}
