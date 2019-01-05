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

package io.github.mirromutth.r2dbc.mysql.message.backend;

import io.github.mirromutth.r2dbc.mysql.constant.HandshakeVersion;

import static java.util.Objects.requireNonNull;

/**
 * A handshake packet message from the MySQL server
 */
abstract class AbstractHandshakeMessage implements BackendMessage {

    private final String serverVersion; // MySQL server version (human readable)
    private final int connectionId; // MySQL server thread id (also connection id)

    AbstractHandshakeMessage(String serverVersion, int connectionId) {
        this.serverVersion = requireNonNull(serverVersion);
        this.connectionId = connectionId;
    }

    public abstract HandshakeVersion getVersion();

    public String getServerVersion() {
        return serverVersion;
    }

    public int getConnectionId() {
        return connectionId;
    }
}
