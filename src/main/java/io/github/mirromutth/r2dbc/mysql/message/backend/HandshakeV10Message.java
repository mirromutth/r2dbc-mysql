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
import io.github.mirromutth.r2dbc.mysql.constant.Capability;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.github.mirromutth.r2dbc.mysql.util.EnumUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.github.mirromutth.r2dbc.mysql.constant.ProtocolConstants.TERMINAL;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL Handshake Message for protocol version 10
 */
public final class HandshakeV10Message extends AbstractHandshakeMessage implements BackendMessage {

    private static final AuthType DEFAULT_AUTH_TYPE = AuthType.MYSQL_NATIVE_PASSWORD;

    private static final int RESERVED_SIZE = 10;

    private static final int MIN_SALT_SECOND_PART_SIZE = 12;

    private final byte[] salt;

    private final int serverCapabilities;

    /**
     * Character collation, MySQL give lower 8-bits only.
     */
    private final byte collationLow8Bits;

    private final short serverStatuses;

    private final AuthType authType; // default is mysql_native_password

    private HandshakeV10Message(
        HandshakeHeader handshakeHeader,
        byte[] salt,
        int serverCapabilities,
        byte collationLow8Bits,
        short serverStatuses,
        AuthType authType
    ) {
        super(handshakeHeader);

        this.salt = requireNonNull(salt, "salt must not be null");
        this.serverCapabilities = serverCapabilities;
        this.collationLow8Bits = collationLow8Bits;
        this.serverStatuses = serverStatuses;
        this.authType = requireNonNull(authType, "authType must not be null");
    }

    static HandshakeV10Message decode(ByteBuf buf, HandshakeHeader handshakeHeader) {
        Builder builder = new Builder().withHandshakeHeader(handshakeHeader);
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
                builder.withCollationLow8Bits(buf.readByte())
                    .withServerStatuses(buf.readShortLE());

                // No need release `capabilities` second part, it will release with `capabilities`
                serverCapabilities = capabilities.addComponent(true, buf.readRetainedSlice(2))
                    .readIntLE();

                builder.withServerCapabilities(serverCapabilities);
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
        short saltSize = 0;
        boolean isPluginAuth = (serverCapabilities & Capability.PLUGIN_AUTH.getFlag()) != 0;

        if (isPluginAuth) {
            saltSize = buf.readUnsignedByte();
        } else {
            buf.skipBytes(1); // if PLUGIN_AUTH flag not exists, MySQL server will return 0x00 always.
        }

        // Reserved field, all bytes are 0x00.
        buf.skipBytes(RESERVED_SIZE);

        int saltSecondPartSize = Math.max(
            MIN_SALT_SECOND_PART_SIZE,
            saltSize - salt.readableBytes() - 1
        );

        ByteBuf saltSecondPart = buf.readSlice(saltSecondPartSize);

        // Always 0x00, and it is not salt part, ignore.
        buf.skipBytes(1);

        // No need release salt second part, it will release with `salt`
        builder.withSalt(ByteBufUtil.getBytes(salt.addComponent(true, saltSecondPart.retain())));

        if (isPluginAuth) {
            if (buf.bytesBefore(TERMINAL) < 0) {
                // It is MySQL bug 59453, auth type native name has no terminal character in
                // version less than 5.5.10, or version greater than 5.6.0 and less than 5.6.2
                // And MySQL only support "mysql_native_password" in those versions,
                // maybe just use constant AuthType.MYSQL_NATIVE_PASSWORD without read?
                builder.withAuthType(EnumUtils.authType(buf.toString(charset)));
            } else {
                builder.withAuthType(EnumUtils.authType(CodecUtils.readCString(buf, charset)));
            }
        } else {
            builder.withAuthType(DEFAULT_AUTH_TYPE);
        }

        return builder;
    }

    public byte[] getSalt() {
        return salt;
    }

    public int getServerCapabilities() {
        return serverCapabilities;
    }

    public byte getCollationLow8Bits() {
        return collationLow8Bits;
    }

    public short getServerStatuses() {
        return serverStatuses;
    }

    public AuthType getAuthType() {
        return authType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HandshakeV10Message)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        HandshakeV10Message that = (HandshakeV10Message) o;

        if (serverCapabilities != that.serverCapabilities) {
            return false;
        }
        if (collationLow8Bits != that.collationLow8Bits) {
            return false;
        }
        if (serverStatuses != that.serverStatuses) {
            return false;
        }
        if (!Arrays.equals(salt, that.salt)) {
            return false;
        }
        return authType == that.authType;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(salt);
        result = 31 * result + serverCapabilities;
        result = 31 * result + (int) collationLow8Bits;
        result = 31 * result + (int) serverStatuses;
        result = 31 * result + (authType != null ? authType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HandshakeV10Message{" +
            "salt=" + Arrays.toString(salt) +
            ", serverCapabilities=" + serverCapabilities +
            ", collationLow8Bits=" + collationLow8Bits +
            ", serverStatuses=" + serverStatuses +
            ", authType=" + authType +
            ", handshakeHeader=" + getHandshakeHeader() +
            '}';
    }

    private static final class Builder {

        private HandshakeHeader handshakeHeader;

        private AuthType authType; // null if PLUGIN_AUTH flag not exists in serverCapabilities

        private byte collationLow8Bits;

        private byte[] salt;

        private int serverCapabilities;

        private short serverStatuses;

        private Builder() {
        }

        HandshakeV10Message build() {
            return new HandshakeV10Message(
                handshakeHeader,
                salt,
                serverCapabilities,
                collationLow8Bits,
                serverStatuses,
                authType
            );
        }

        void withAuthType(AuthType authType) {
            this.authType = authType;
        }

        Builder withCollationLow8Bits(byte collationLow8Bits) {
            this.collationLow8Bits = collationLow8Bits;
            return this;
        }

        Builder withHandshakeHeader(HandshakeHeader handshakeHeader) {
            this.handshakeHeader = handshakeHeader;
            return this;
        }

        void withSalt(byte[] salt) {
            this.salt = salt;
        }

        void withServerCapabilities(int serverCapabilities) {
            this.serverCapabilities = serverCapabilities;
        }

        void withServerStatuses(short serverStatuses) {
            this.serverStatuses = serverStatuses;
        }
    }
}
