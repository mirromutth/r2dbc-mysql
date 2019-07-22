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

package io.github.mirromutth.r2dbc.mysql.message.server;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcPermissionDeniedException;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * A handshake message from the MySQL server
 */
public abstract class AbstractHandshakeMessage implements ServerMessage {

    private final HandshakeHeader header;

    AbstractHandshakeMessage(HandshakeHeader header) {
        this.header = requireNonNull(header, "header must not be null");
    }

    public final HandshakeHeader getHeader() {
        return header;
    }

    abstract public int getServerCapabilities();

    abstract public String getAuthType();

    abstract public byte[] getSalt();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractHandshakeMessage)) {
            return false;
        }

        AbstractHandshakeMessage that = (AbstractHandshakeMessage) o;

        return header.equals(that.header);
    }

    @Override
    public int hashCode() {
        return header.hashCode();
    }

    @Override
    public String toString() {
        return "AbstractHandshakeMessage{" +
            "header=" + header +
            '}';
    }

    static AbstractHandshakeMessage decode(ByteBuf buf) {
        HandshakeHeader header = HandshakeHeader.decode(buf);
        int version = header.getProtocolVersion();

        switch (version) {
            case 10:
                return HandshakeV10Message.decodeV10(buf, header);
            case 9:
                return HandshakeV9Message.decodeV9(buf, header);
            default:
                throw new R2dbcPermissionDeniedException(String.format("Handshake protocol version %d not support.", version));
        }
    }
}
