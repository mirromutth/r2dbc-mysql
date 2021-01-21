/*
 * Copyright 2018-2021 the original author or authors.
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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.Capability;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * An abstraction of {@link ClientMessage} that considers SSL request for handshake.
 */
public interface SslRequest extends LoginClientMessage {

    /**
     * Get current {@link Capability} of the connection.
     *
     * @return the {@link Capability}.
     */
    Capability getCapability();

    /**
     * Construct an instance of {@link SslRequest}, it is implemented by the protocol version that is given by
     * {@link Capability}.
     *
     * @param envelopeId  the beginning envelope ID of this message.
     * @param capability  the current {@link Capability}.
     * @param collationId the {@code CharCollation} ID, or 0 if server does not return a collation ID.
     * @return the instance implemented by the specified protocol version.
     */
    static SslRequest from(int envelopeId, Capability capability, int collationId) {
        require(capability.isSslEnabled(), "capability must be SSL enabled");

        if (capability.isProtocol41()) {
            return new SslRequest41(envelopeId, capability, collationId);
        }

        return new SslRequest320(envelopeId, capability);
    }
}
