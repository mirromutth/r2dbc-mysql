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

import io.github.mirromutth.r2dbc.mysql.constant.AuthType;
import io.github.mirromutth.r2dbc.mysql.constant.HandshakeVersion;

import static java.util.Objects.requireNonNull;

/**
 * MySQL Handshake Packet for protocol version 10
 */
public final class HandshakeV10Message extends AbstractHandshakeMessage {

    private final byte[] seed;
    private final int serverCapabilities;
    private final int charCollation;
    private final int serverStatuses;
    private final AuthType authType;

    public HandshakeV10Message(
            String serverVersion,
            int connectionId,
            byte[] seed,
            int serverCapabilities,
            int charCollation,
            int serverStatuses,
            AuthType authType
    ) {
        super(serverVersion, connectionId);

        this.seed = requireNonNull(seed);
        this.serverCapabilities = serverCapabilities;
        this.charCollation = charCollation;
        this.serverStatuses = serverStatuses;
        this.authType = requireNonNull(authType);
    }

    @Override
    public HandshakeVersion getVersion() {
        return HandshakeVersion.V10;
    }
}
