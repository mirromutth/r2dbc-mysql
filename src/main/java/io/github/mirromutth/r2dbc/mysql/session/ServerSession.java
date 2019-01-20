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

package io.github.mirromutth.r2dbc.mysql.session;

import io.github.mirromutth.r2dbc.mysql.constant.AuthType;

import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL server sessions.
 */
public final class ServerSession {

    private final int connectionId;

    private final ServerVersion serverVersion;

    private final int serverCapabilities;

    private final AuthType authType;

    private final Charset charset;

    public ServerSession(
        int connectionId,
        ServerVersion serverVersion,
        int serverCapabilities,
        AuthType authType,
        Charset charset
    ) {
        this.connectionId = connectionId;
        this.serverVersion = requireNonNull(serverVersion, "serverVersion must not be null");
        this.serverCapabilities = serverCapabilities;
        this.authType = requireNonNull(authType, "authType must not be null");
        this.charset = requireNonNull(charset, "charset must not be null");
    }

    public int getConnectionId() {
        return connectionId;
    }

    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    public int getServerCapabilities() {
        return serverCapabilities;
    }

    public AuthType getAuthType() {
        return authType;
    }

    public Charset getCharset() {
        return charset;
    }
}
