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

import dev.miku.r2dbc.mysql.constant.AuthTypes;
import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.internal.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static dev.miku.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * MySQL Handshake Message for protocol version 10.
 */
final class HandshakeV10Request implements HandshakeRequest, ServerStatusMessage {

    private static final int RESERVED_SIZE = 10;

    private static final int MIN_SALT_SECOND_PART_SIZE = 12;

    private final HandshakeHeader header;

    private final byte[] salt;

    private final int serverCapabilities;

    /**
     * Character collation, MySQL give lower 8-bits only.
     * Try NOT use this.
     */
    private final byte collationLow8Bits;

    private final short serverStatuses;

    private final String authType; // default is mysql_native_password

    private HandshakeV10Request(
        HandshakeHeader header, byte[] salt, int serverCapabilities,
        byte collationLow8Bits, short serverStatuses, String authType
    ) {
        this.header = requireNonNull(header, "header must not be null");
        this.salt = requireNonNull(salt, "salt must not be null");
        this.serverCapabilities = serverCapabilities;
        this.collationLow8Bits = collationLow8Bits;
        this.serverStatuses = serverStatuses;
        this.authType = requireNonNull(authType, "authType must not be null");
    }

    @Override
    public HandshakeHeader getHeader() {
        return header;
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    @Override
    public int getServerCapabilities() {
        return serverCapabilities;
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
        if (!(o instanceof HandshakeV10Request)) {
            return false;
        }

        HandshakeV10Request that = (HandshakeV10Request) o;

        if (serverCapabilities != that.serverCapabilities) {
            return false;
        }
        if (collationLow8Bits != that.collationLow8Bits) {
            return false;
        }
        if (serverStatuses != that.serverStatuses) {
            return false;
        }
        if (!header.equals(that.header)) {
            return false;
        }
        if (!Arrays.equals(salt, that.salt)) {
            return false;
        }
        return authType.equals(that.authType);
    }

    @Override
    public int hashCode() {
        int result = header.hashCode();
        result = 31 * result + Arrays.hashCode(salt);
        result = 31 * result + serverCapabilities;
        result = 31 * result + (int) collationLow8Bits;
        result = 31 * result + (int) serverStatuses;
        result = 31 * result + authType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("HandshakeV10Request{header=%s, salt=REDACTED, serverCapabilities=%x, collationLow8Bits=%d, serverStatuses=%x, authType=%s}",
            header, serverCapabilities, collationLow8Bits, serverStatuses, authType);
    }

    static HandshakeV10Request decodeV10(ByteBuf buf, HandshakeHeader header) {
        Builder builder = new Builder().header(header);
        CompositeByteBuf salt = buf.alloc().compositeBuffer(2);

        try {
            // After handshake header, MySQL give salt first part (should be 8-bytes always).
            salt.addComponent(true, CodecUtils.readCStringSlice(buf).retain());

            int serverCapabilities;
            CompositeByteBuf capabilities = buf.alloc().compositeBuffer(2);

            try {
                // After salt first part, MySQL give the Server Capabilities first part (always 2-bytes).
                capabilities.addComponent(true, buf.readRetainedSlice(2));

                // New protocol with 16 bytes to describe server character, but MySQL give lower 8-bits only.
                builder.collationLow8Bits(buf.readByte())
                    .serverStatuses(buf.readShortLE());

                // No need release `capabilities` second part, it will release with `capabilities`
                serverCapabilities = capabilities.addComponent(true, buf.readRetainedSlice(2))
                    .readIntLE();

                builder.serverCapabilities(serverCapabilities);
            } finally {
                capabilities.release();
            }

            return afterCapabilities(builder, buf, serverCapabilities, salt).build();
        } finally {
            salt.release();
        }
    }

    private static Builder afterCapabilities(Builder builder, ByteBuf buf, int serverCapabilities, CompositeByteBuf salt) {
        // Special charset on handshake process, just use ascii.
        Charset charset = StandardCharsets.US_ASCII;
        short saltSize;
        boolean isPluginAuth = (serverCapabilities & Capabilities.PLUGIN_AUTH) != 0;

        if (isPluginAuth) {
            saltSize = buf.readUnsignedByte();
        } else {
            saltSize = 0;
            buf.skipBytes(1); // if PLUGIN_AUTH flag not exists, MySQL server will return 0x00 always.
        }

        // Reserved field, all bytes are 0x00.
        buf.skipBytes(RESERVED_SIZE);

        if ((serverCapabilities & Capabilities.SECURE_CONNECTION) != 0) {
            int saltSecondPartSize = Math.max(MIN_SALT_SECOND_PART_SIZE, saltSize - salt.readableBytes() - 1);
            ByteBuf saltSecondPart = buf.readSlice(saltSecondPartSize);
            // Always 0x00, and it is not the part of salt, ignore.
            buf.skipBytes(1);

            // No need release salt second part, it will release with `salt`.
            salt.addComponent(true, saltSecondPart.retain());
        }

        builder.salt(ByteBufUtil.getBytes(salt));

        if (isPluginAuth) {
            if (CodecUtils.hasNextCString(buf)) {
                builder.authType(CodecUtils.readCString(buf, charset));
            } else {
                // It is MySQL bug 59453, auth type native name has no terminal character in
                // version less than 5.5.10, or version greater than 5.6.0 and less than 5.6.2
                // And MySQL only support "mysql_native_password" in those versions,
                // maybe just use constant "mysql_native_password" without read?
                builder.authType(buf.toString(charset));
            }
        } else {
            builder.authType(AuthTypes.NO_AUTH_PROVIDER);
        }

        return builder;
    }

    private static final class Builder {

        private HandshakeHeader header;

        private String authType; // null if PLUGIN_AUTH flag not exists in serverCapabilities

        private byte collationLow8Bits;

        private byte[] salt;

        private int serverCapabilities;

        private short serverStatuses;

        private Builder() {
        }

        HandshakeV10Request build() {
            return new HandshakeV10Request(header, salt, serverCapabilities, collationLow8Bits, serverStatuses, authType);
        }

        void authType(String authType) {
            this.authType = authType;
        }

        Builder collationLow8Bits(byte collationLow8Bits) {
            this.collationLow8Bits = collationLow8Bits;
            return this;
        }

        Builder header(HandshakeHeader header) {
            this.header = header;
            return this;
        }

        void salt(byte[] salt) {
            this.salt = salt;
        }

        void serverCapabilities(int serverCapabilities) {
            this.serverCapabilities = serverCapabilities;
        }

        void serverStatuses(short serverStatuses) {
            this.serverStatuses = serverStatuses;
        }
    }
}
