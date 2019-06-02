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
import reactor.util.annotation.NonNull;

import static io.github.mirromutth.r2dbc.mysql.constant.AuthTypes.SHA256_PASSWORD;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.internal.EmptyArrays.EMPTY_BYTES;

/**
 * A MySQL authentication state machine which would use when authentication type is "sha256_password".
 */
final class Sha256AuthStateMachine implements AuthStateMachine {

    private static final Sha256AuthStateMachine INSTANCE = new Sha256AuthStateMachine();

    static Sha256AuthStateMachine getInstance() {
        return INSTANCE;
    }

    private Sha256AuthStateMachine() {
    }

    @Override
    public boolean hasNext() {
        // TODO: "sha256_password" is not state machine?
        return false;
    }

    @Override
    public boolean isSslNecessary() {
        return true;
    }

    @NonNull
    @Override
    public byte[] nextAuthentication(MySqlSession session) {
        requireNonNull(session, "session must not be null");

        String password = session.getPassword();

        if (password == null || password.isEmpty()) {
            return EMPTY_BYTES;
        }

        // TODO: implement

        return EMPTY_BYTES;
    }

    @Override
    public String getType() {
        return SHA256_PASSWORD;
    }
}
