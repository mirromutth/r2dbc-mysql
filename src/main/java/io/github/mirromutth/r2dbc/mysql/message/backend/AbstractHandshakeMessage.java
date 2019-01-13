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

import io.github.mirromutth.r2dbc.mysql.constant.ProtocolVersion;
import io.github.mirromutth.r2dbc.mysql.exception.ProtocolNotSupportException;
import io.netty.buffer.ByteBuf;

import static java.util.Objects.requireNonNull;

/**
 * A handshake message from the MySQL server
 */
public abstract class AbstractHandshakeMessage implements BackendMessage {

    private final HandshakeHeader handshakeHeader;

    AbstractHandshakeMessage(HandshakeHeader handshakeHeader) {
        this.handshakeHeader = requireNonNull(handshakeHeader, "handshakeHeader must not be null");
    }

    public final HandshakeHeader getHandshakeHeader() {
        return handshakeHeader;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractHandshakeMessage)) {
            return false;
        }

        AbstractHandshakeMessage that = (AbstractHandshakeMessage) o;

        return handshakeHeader.equals(that.handshakeHeader);
    }

    @Override
    public int hashCode() {
        return handshakeHeader.hashCode();
    }

    @Override
    public String toString() {
        return "AbstractHandshakeMessage{" +
            "handshakeHeader=" + handshakeHeader +
            '}';
    }

    static AbstractHandshakeMessage decode(ByteBuf buf) {
        HandshakeHeader handshakeHeader = HandshakeHeader.decode(buf);
        ProtocolVersion protocolVersion = handshakeHeader.getProtocolVersion();

        if (ProtocolVersion.V10.equals(protocolVersion)) {
            return HandshakeV10Message.decode(buf, handshakeHeader);
        }

        throw new ProtocolNotSupportException(protocolVersion.getCode());
    }
}
