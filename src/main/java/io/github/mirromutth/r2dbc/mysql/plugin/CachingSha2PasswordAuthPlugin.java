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

package io.github.mirromutth.r2dbc.mysql.plugin;

import io.github.mirromutth.r2dbc.mysql.constant.AuthType;
import io.github.mirromutth.r2dbc.mysql.util.EmptyArrays;
import reactor.util.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * MySQL Authentication Plugin for "caching_sha2_password"
 */
public final class CachingSha2PasswordAuthPlugin implements AuthPlugin {

    private static final CachingSha2PasswordAuthPlugin INSTANCE = new CachingSha2PasswordAuthPlugin();

    public static CachingSha2PasswordAuthPlugin getInstance() {
        return INSTANCE;
    }

    private CachingSha2PasswordAuthPlugin() {
    }

    @Override
    public AuthType getType() {
        return AuthType.CACHING_SHA2_PASSWORD;
    }

    @Override
    public byte[] encrypt(@Nullable byte[] password, byte[] scramble) {
        if (password == null) {
            return EmptyArrays.EMPTY_BYTES;
        }

        requireNonNull(scramble);

        // TODO: implement this method

        return new byte[0];
    }
}
