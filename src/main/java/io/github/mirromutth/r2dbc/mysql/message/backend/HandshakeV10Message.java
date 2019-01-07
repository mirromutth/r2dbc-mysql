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
import io.github.mirromutth.r2dbc.mysql.constant.Handshakes;
import io.github.mirromutth.r2dbc.mysql.constant.ServerCapability;
import io.github.mirromutth.r2dbc.mysql.message.PacketHeader;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.github.mirromutth.r2dbc.mysql.util.EnumUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * MySQL Handshake Packet for protocol version 10
 */
public final class HandshakeV10Message extends AbstractHandshakeMessage implements BackendMessage {

    private final byte[] scramble;

    private final int serverCapabilities;

    /**
     * Character collation, MySQL give lower 8-bits only.
     */
    private final byte collationLow8Bits;

    private final short serverStatuses;

    @Nullable
    private final AuthType authType; // null if PLUGIN_AUTH flag not exists in serverCapabilities

    private HandshakeV10Message(
        PacketHeader packetHeader,
        HandshakeHeader handshakeHeader,
        byte[] scramble,
        int serverCapabilities,
        byte collationLow8Bits,
        short serverStatuses,
        @Nullable AuthType authType
    ) {
        super(packetHeader, handshakeHeader);

        this.scramble = requireNonNull(scramble);
        this.serverCapabilities = serverCapabilities;
        this.collationLow8Bits = collationLow8Bits;
        this.serverStatuses = serverStatuses;
        this.authType = authType;
    }

    public static HandshakeV10Message decode(ByteBuf buf) {
        Builder builder = new Builder().withPacketHeader(CodecUtils.readPacketHeader(buf))
            .withHandshakeHeader(HandshakeHeader.decode(buf));
        CompositeByteBuf scramble = Unpooled.compositeBuffer(2);

        try {
            // After handshake header, MySQL give scramble first part (should be 8-bytes always).
            ByteBuf scrambleFirstPart = CodecUtils.readCString(buf);

            // No need release `scrambleFirstPart`, it will release with `scramble`
            scramble.addComponent(true, scrambleFirstPart);

            return afterScrambleFirstPart(builder, buf, scramble, scrambleFirstPart.readableBytes()).build();
        } finally {
            ReferenceCountUtil.release(scramble);
        }
    }

    private static Builder afterScrambleFirstPart(
        Builder builder,
        ByteBuf buf,
        CompositeByteBuf scramble,
        int scrambleFirstPartSize
    ) {
        CompositeByteBuf capabilities = Unpooled.compositeBuffer(2);

        try {
            // After scramble first part, MySQL give the Server Capabilities first part (always 2-bytes).
            // No need release `capabilities` first part, it will release with `capabilities`
            capabilities.addComponent(true, buf.readBytes(2));

            // New protocol with 16 bytes to describe server character, but MySQL give lower 8-bits only.
            builder.withCollationLow8Bits(buf.readByte())
                .withServerStatuses(buf.readShortLE());

            // No need release `capabilities` second part, it will release with `capabilities`
            int serverCapabilities = capabilities.addComponent(true, buf.readBytes(2))
                .readIntLE();

            builder.withServerCapabilities(serverCapabilities);

            short scrambleSize = 0;

            boolean isPluginAuth = (serverCapabilities & ServerCapability.PLUGIN_AUTH.getFlag()) != 0;

            if (isPluginAuth) {
                scrambleSize = buf.readUnsignedByte();
            } else {
                buf.skipBytes(1); // if PLUGIN_AUTH flag not exists, MySQL server will return 0x00 always.
            }

            // Reserved field, all bytes are 0x00.
            buf.skipBytes(Handshakes.RESERVED_SIZE);

            int scrambleSecondPartSize = Math.max(
                Handshakes.MIN_SCRAMBLE_SECOND_PART_SIZE,
                scrambleSize - scrambleFirstPartSize - 1
            );

            ByteBuf scrambleSecondPart = buf.readBytes(scrambleSecondPartSize);

            // Always 0x00, and it is not scramble part, ignore.
            buf.skipBytes(1);

            // No need release scramble second part, it will release with `scramble`
            builder.withScramble(ByteBufUtil.getBytes(scramble.addComponent(true, scrambleSecondPart)));

            if (isPluginAuth) {
                ByteBuf authTypeName = CodecUtils.readCString(buf);

                try {
                    builder.withAuthType(EnumUtils.authType(authTypeName.toString(Charset.defaultCharset())));
                } finally {
                    ReferenceCountUtil.release(authTypeName);
                }
            }

            return builder;
        } finally {
            ReferenceCountUtil.release(capabilities);
        }
    }

    public byte[] getScramble() {
        return scramble;
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

    @Nullable
    public AuthType getAuthType() {
        return authType;
    }

    @Override
    public String toString() {
        return "HandshakeV10Message{" +
            "scramble=" + Arrays.toString(scramble) +
            ", serverCapabilities=" + serverCapabilities +
            ", collationLow8Bits=" + collationLow8Bits +
            ", serverStatuses=" + serverStatuses +
            ", authType=" + authType +
            ", handshakeHeader=" + getHandshakeHeader() +
            ", packetHeader=" + getPacketHeader() +
            '}';
    }

    private static final class Builder {

        private PacketHeader packetHeader;

        private HandshakeHeader handshakeHeader;

        private AuthType authType; // null if PLUGIN_AUTH flag not exists in serverCapabilities

        private byte collationLow8Bits;

        private byte[] scramble;

        private int serverCapabilities;

        private short serverStatuses;

        private Builder() {
        }

        HandshakeV10Message build() {
            return new HandshakeV10Message(
                packetHeader,
                handshakeHeader,
                scramble,
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

        Builder withPacketHeader(PacketHeader packetHeader) {
            this.packetHeader = packetHeader;
            return this;
        }

        void withScramble(byte[] scramble) {
            this.scramble = scramble;
        }

        void withServerCapabilities(int serverCapabilities) {
            this.serverCapabilities = serverCapabilities;
        }

        void withServerStatuses(short serverStatuses) {
            this.serverStatuses = serverStatuses;
        }
    }
}
