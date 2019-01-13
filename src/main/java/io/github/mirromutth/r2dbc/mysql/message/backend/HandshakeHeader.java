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
import io.github.mirromutth.r2dbc.mysql.session.ServerVersion;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.github.mirromutth.r2dbc.mysql.util.EnumUtils;
import io.netty.buffer.ByteBuf;

import static java.util.Objects.requireNonNull;

/**
 * The handshake generic header, all protocol versions contains
 * this data whether V9 or V10.
 * <p>
 * If MySQL protocol will be released V11 or higher versions,
 * may need to reorganize the generic headers.
 */
public final class HandshakeHeader {

    private final ProtocolVersion protocolVersion;

    private final ServerVersion serverVersion;

    private final int connectionId;

    private HandshakeHeader(ProtocolVersion protocolVersion, ServerVersion serverVersion, int connectionId) {
        this.protocolVersion = requireNonNull(protocolVersion, "protocolVersion must not be null");
        this.serverVersion = requireNonNull(serverVersion, "serverVersion must not be null");
        this.connectionId = connectionId;
    }

    /**
     * The first byte defines the MySQL protocol version those
     * permit the MySQL server to add support for newer protocols.
     *
     * @return The protocol version by MySQL server used.
     */
    public ProtocolVersion getProtocolVersion() {
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
        ProtocolVersion protocolVersion = EnumUtils.protocolVersion(buf.readUnsignedByte());
        ServerVersion serverVersion = ServerVersion.parse(CodecUtils.readCStringSlice(buf));
        return new HandshakeHeader(protocolVersion, serverVersion, buf.readIntLE());
    }

    @Override
    public String toString() {
        return "HandshakeHeader{" +
            "protocolVersion=" + protocolVersion +
            ", serverVersion=" + serverVersion +
            ", connectionId=" + connectionId +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HandshakeHeader that = (HandshakeHeader) o;

        if (connectionId != that.connectionId) {
            return false;
        }
        if (protocolVersion != that.protocolVersion) {
            return false;
        }
        return serverVersion.equals(that.serverVersion);
    }

    @Override
    public int hashCode() {
        int result = protocolVersion.hashCode();
        result = 31 * result + serverVersion.hashCode();
        result = 31 * result + connectionId;
        return result;
    }
}
