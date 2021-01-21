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
import dev.miku.r2dbc.mysql.authentication.MySqlAuthProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL Handshake Message for protocol version 10.
 */
final class HandshakeV10Request implements HandshakeRequest, ServerStatusMessage {

    private static final int RESERVED_SIZE = 10;

    private static final int SALT_FIRST_PART_SIZE = 8;

    private static final int MIN_SALT_SECOND_PART_SIZE = 12;

    private final HandshakeHeader header;

    private final int envelopeId;

    private final byte[] salt;

    private final Capability serverCapability;

    private final short serverStatuses;

    private final String authType;

    private HandshakeV10Request(HandshakeHeader header, int envelopeId, byte[] salt,
        Capability serverCapability, short serverStatuses, String authType) {
        this.header = requireNonNull(header, "header must not be null");
        this.envelopeId = envelopeId;
        this.salt = requireNonNull(salt, "salt must not be null");
        this.serverCapability = requireNonNull(serverCapability, "serverCapability must not be null");
        this.serverStatuses = serverStatuses;
        this.authType = requireNonNull(authType, "authType must not be null");
    }

    @Override
    public HandshakeHeader getHeader() {
        return header;
    }

    @Override
    public int getEnvelopeId() {
        return envelopeId;
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    @Override
    public Capability getServerCapability() {
        return serverCapability;
    }

    @Override
    public short getServerStatuses() {
        return serverStatuses;
    }

    @Override
    public String getAuthType() {
        return authType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HandshakeV10Request that = (HandshakeV10Request) o;

        return envelopeId == that.envelopeId && serverStatuses == that.serverStatuses &&
            header.equals(that.header) && Arrays.equals(salt, that.salt) &&
            serverCapability.equals(that.serverCapability) && authType.equals(that.authType);
    }

    @Override
    public int hashCode() {
        int hash = 31 * header.hashCode() + envelopeId;
        hash = 31 * hash + Arrays.hashCode(salt);
        hash = 31 * hash + serverCapability.hashCode();
        hash = 31 * hash + serverStatuses;
        return 31 * hash + authType.hashCode();
    }

    @Override
    public String toString() {
        return "HandshakeV10Request{header=" + header + ", envelopeId=" + envelopeId +
            ", salt=REDACTED, serverCapability=" + serverCapability +
            ", serverStatuses=" + serverStatuses + ", authType='" + authType + "'}";
    }

    static HandshakeV10Request decode(int envelopeId, ByteBuf buf, HandshakeHeader header) {
        Builder builder = new Builder(envelopeId, header);
        ByteBuf salt = buf.alloc().buffer();

        try {
            // The salt first part after handshake header, always 8 bytes.
            salt.writeBytes(buf, buf.readerIndex(), SALT_FIRST_PART_SIZE);
            // Skip slat first part and terminal.
            buf.skipBytes(SALT_FIRST_PART_SIZE + 1);

            // The Server Capabilities first part following the salt first part. (always lower 2-bytes)
            int loCapabilities = buf.readUnsignedShortLE();

            // MySQL is using 16 bytes to identify server character. There has lower 8-bits only, skip it.
            buf.skipBytes(1);
            builder.serverStatuses(buf.readShortLE());

            // The Server Capabilities second part following the server statuses. (always upper 2-bytes)
            int hiCapabilities = buf.readUnsignedShortLE() << Short.SIZE;
            Capability capability = Capability.of(loCapabilities | hiCapabilities);

            builder.serverCapability(capability);

            // If PLUGIN_AUTH flag not exists, MySQL server will return 0x00 always.
            short saltSize = buf.readUnsignedByte();

            // Reserved field, all bytes are 0x00.
            buf.skipBytes(RESERVED_SIZE);

            if (capability.isSaltSecured()) {
                // If it has not this part, means it is using mysql_old_password,
                // that salt and authentication is not secure.
                int saltSecondPartSize = Math.max(MIN_SALT_SECOND_PART_SIZE,
                    saltSize - SALT_FIRST_PART_SIZE - 1);

                salt.writeBytes(buf, buf.readerIndex(), saltSecondPartSize);
                // Skip salt second part and terminal.
                buf.skipBytes(saltSecondPartSize + 1);
            }

            builder.salt(ByteBufUtil.getBytes(salt));

            if (capability.isPluginAuthAllowed()) {
                // See also MySQL bug 59453, auth type native name has no terminal character in
                // version less than 5.5.10, or version greater than 5.6.0 and less than 5.6.2
                // And MySQL only support "mysql_native_password" in those versions that has the
                // bug, maybe just use constant "mysql_native_password" without read?
                int length = buf.bytesBefore(TERMINAL);

                if (length < 0) {
                    builder.authType(buf.toString(StandardCharsets.US_ASCII));
                } else {
                    builder.authType(length == 0 ? MySqlAuthProvider.NO_AUTH_PROVIDER :
                        buf.toString(buf.readerIndex(), length, StandardCharsets.US_ASCII));
                }
            } else {
                builder.authType(MySqlAuthProvider.NO_AUTH_PROVIDER);
            }

            return builder.build();
        } finally {
            salt.release();
        }
    }

    private static final class Builder {

        private final int envelopeId;

        private final HandshakeHeader header;

        private String authType;

        private byte[] salt;

        private Capability serverCapability;

        private short serverStatuses;

        private Builder(int envelopeId, HandshakeHeader header) {
            this.envelopeId = envelopeId;
            this.header = header;
        }

        HandshakeV10Request build() {
            return new HandshakeV10Request(header, envelopeId, salt, serverCapability, serverStatuses,
                authType);
        }

        void authType(String authType) {
            this.authType = authType;
        }

        void salt(byte[] salt) {
            this.salt = salt;
        }

        void serverCapability(Capability serverCapability) {
            this.serverCapability = serverCapability;
        }

        void serverStatuses(short serverStatuses) {
            this.serverStatuses = serverStatuses;
        }
    }
}
