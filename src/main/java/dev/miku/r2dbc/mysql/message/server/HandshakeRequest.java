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

package dev.miku.r2dbc.mysql.message.server;

import dev.miku.r2dbc.mysql.Capability;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcPermissionDeniedException;

/**
 * A MySQL Handshake Request message for multi-versions.
 */
public interface HandshakeRequest extends ServerMessage {

    /**
     * Get the handshake request header.
     *
     * @return the header.
     */
    HandshakeHeader getHeader();

    /**
     * Get the envelope identifier of this message packet.
     *
     * @return envelope identifier.
     */
    int getEnvelopeId();

    /**
     * Get the server-side capability.
     *
     * @return the server-side capability.
     */
    Capability getServerCapability();

    /**
     * Get the authentication plugin type name.
     *
     * @return the authentication plugin type.
     */
    String getAuthType();

    /**
     * Get the challenge salt for authentication.
     *
     * @return the challenge salt.
     */
    byte[] getSalt();

    /**
     * Decode a {@link HandshakeRequest} from a envelope {@link ByteBuf}.
     *
     * @param envelopeId envelope identifier.
     * @param buf        the {@link ByteBuf}.
     * @return decoded {@link HandshakeRequest}.
     */
    static HandshakeRequest decode(int envelopeId, ByteBuf buf) {
        HandshakeHeader header = HandshakeHeader.decode(buf);
        int version = header.getProtocolVersion();

        switch (version) {
            case 10:
                return HandshakeV10Request.decode(envelopeId, buf, header);
            case 9:
                return HandshakeV9Request.decode(envelopeId, buf, header);
        }

        throw new R2dbcPermissionDeniedException("Does not support handshake protocol version " + version);
    }
}
