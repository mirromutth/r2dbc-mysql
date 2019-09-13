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

package dev.miku.r2dbc.mysql.message.server;

import dev.miku.r2dbc.mysql.ServerVersion;
import dev.miku.r2dbc.mysql.internal.CodecUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static dev.miku.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * The handshake generic header, all protocol versions contains
 * this data whether V9 or V10.
 * <p>
 * If MySQL protocol will be released V11 or higher versions,
 * may need to reorganize the generic headers.
 */
public final class HandshakeHeader {

    private final short protocolVersion;

    private final ServerVersion serverVersion;

    private final int connectionId;

    private HandshakeHeader(short protocolVersion, ServerVersion serverVersion, int connectionId) {
        this.protocolVersion = protocolVersion;
        this.serverVersion = requireNonNull(serverVersion, "serverVersion must not be null");
        this.connectionId = connectionId;
    }

    /**
     * The first byte defines the MySQL handshake version those
     * permit the MySQL server to add support for newer protocols.
     *
     * @return The handshake version by MySQL server used.
     */
    public short getProtocolVersion() {
        return protocolVersion;
    }

    /**
     * @return MySQL server version
     */
    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    /**
     * @return The connection id by MySQL server given, can NOT promise positive integer.
     */
    public int getConnectionId() {
        return connectionId;
    }

    static HandshakeHeader decode(ByteBuf buf) {
        short protocolVersion = buf.readUnsignedByte();
        ServerVersion serverVersion = ServerVersion.parse(CodecUtils.readCStringSlice(buf).toString(StandardCharsets.US_ASCII));
        return new HandshakeHeader(protocolVersion, serverVersion, buf.readIntLE());
    }

    @Override
    public String toString() {
        return String.format("HandshakeHeader{protocolVersion=%d, serverVersion=%s, connectionId=%d}", protocolVersion, serverVersion, connectionId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HandshakeHeader)) {
            return false;
        }

        HandshakeHeader that = (HandshakeHeader) o;

        if (protocolVersion != that.protocolVersion) {
            return false;
        }
        if (connectionId != that.connectionId) {
            return false;
        }
        return serverVersion.equals(that.serverVersion);
    }

    @Override
    public int hashCode() {
        int result = protocolVersion;
        result = 31 * result + serverVersion.hashCode();
        result = 31 * result + connectionId;
        return result;
    }
}
