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

import dev.miku.r2dbc.mysql.ServerVersion;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * The handshake generic header, all protocol versions contains this data whether V9 or V10.
 * <p>
 * If MySQL protocol will be released V11 or higher versions, may need to reorganize the generic headers.
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
     * The first byte defines the MySQL handshake version those permit the MySQL server to add support for
     * newer protocols.
     *
     * @return The handshake version by MySQL server used.
     */
    public short getProtocolVersion() {
        return protocolVersion;
    }

    /**
     * Get the MySQL server version.
     *
     * @return MySQL server version.
     */
    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    /**
     * Get the connection identifier by MySQL server given, it may not be a positive integer.
     *
     * @return the connection identifier.
     */
    public int getConnectionId() {
        return connectionId;
    }

    static HandshakeHeader decode(ByteBuf buf) {
        short protocolVersion = buf.readUnsignedByte();
        ServerVersion serverVersion = ServerVersion.parse(readCStringAscii(buf));
        return new HandshakeHeader(protocolVersion, serverVersion, buf.readIntLE());
    }

    @Override
    public String toString() {
        return "HandshakeHeader{protocolVersion=" + protocolVersion + ", serverVersion=" + serverVersion +
            ", connectionId=" + connectionId + '}';
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

        return protocolVersion == that.protocolVersion && connectionId == that.connectionId &&
            serverVersion.equals(that.serverVersion);
    }

    @Override
    public int hashCode() {
        int hash = 31 * protocolVersion + serverVersion.hashCode();
        return 31 * hash + connectionId;
    }

    static String readCStringAscii(ByteBuf buf) {
        int length = buf.bytesBefore(TERMINAL);

        if (length < 0) {
            throw new IllegalArgumentException("buf has no C-style string terminal");
        }

        if (length == 0) {
            // skip terminal
            buf.skipBytes(1);
            return "";
        }

        String result = buf.toString(buf.readerIndex(), length, StandardCharsets.US_ASCII);
        buf.skipBytes(length + 1); // skip string and terminal by read

        return result;
    }
}
