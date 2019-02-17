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

package io.github.mirromutth.r2dbc.mysql.message.frontend;

import io.github.mirromutth.r2dbc.mysql.constant.AuthType;
import io.github.mirromutth.r2dbc.mysql.constant.Capability;
import io.github.mirromutth.r2dbc.mysql.constant.DecodeMode;
import io.github.mirromutth.r2dbc.mysql.constant.ProtocolConstants;
import io.github.mirromutth.r2dbc.mysql.core.ServerSession;
import io.github.mirromutth.r2dbc.mysql.exception.AuthenticationTooLongException;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A handshake response message sent by clients those supporting
 * {@link Capability#PROTOCOL_41} if the server announced it in
 * it's {@code HandshakeV10Message}, otherwise talking to an old
 * server should use the {@code HandshakeResponse320Message}.
 */
public final class HandshakeResponse41Message extends AbstractFrontendMessage {

    private static final int ONE_BYTE_MAX_INT = 0xFF;

    private static final int FILTER_SIZE = 23;

    private final int clientCapabilities;

    private final byte collationLow8Bits;

    private final String username;

    private final byte[] authentication;

    private final AuthType authType;

    private final String database;

    private final Map<String, String> attributes;

    private final boolean varIntSizedAuth;

    public HandshakeResponse41Message(
        int clientCapabilities,
        byte collationLow8Bits,
        String username,
        byte[] authentication,
        AuthType authType,
        @Nullable String database,
        @Nullable Map<String, String> attributes
    ) {
        this.collationLow8Bits = collationLow8Bits;
        this.username = requireNonNull(username, "username must not be null");
        this.authentication = requireNonNull(authentication, "authentication must not be null");

        if (database == null) {
            this.database = "";
        } else {
            this.database = database;
        }

        this.authType = requireNonNull(authType, "authType must not be null");

        if (attributes == null || attributes.isEmpty()) {
            this.attributes = Collections.emptyMap();
        } else {
            this.attributes = attributes;
        }

        // must calculate client capabilities after other properties set.
        this.clientCapabilities = calculateCapabilities(clientCapabilities);
        this.varIntSizedAuth = (this.clientCapabilities & Capability.PLUGIN_AUTH_VAR_INT_SIZED_DATA.getFlag()) != 0;

        // authentication can not longer than 255 if server is not support use var int encode authentication
        if (!this.varIntSizedAuth && authentication.length > ONE_BYTE_MAX_INT) {
            throw new AuthenticationTooLongException(authentication.length);
        }
    }

    @Override
    public DecodeMode responseDecodeMode() {
        return DecodeMode.RESPONSE;
    }

    @Override
    protected ByteBuf encodeSingle(ByteBufAllocator bufAllocator, ServerSession session) {
        final ByteBuf buf = bufAllocator.buffer();
        Charset charset = session.getCollation().getCharset();

        try {
            buf.writeIntLE(clientCapabilities)
                .writeIntLE(ProtocolConstants.MAX_PART_SIZE)
                .writeByte(collationLow8Bits)
                .writeZero(FILTER_SIZE);

            CodecUtils.writeCString(buf, username, charset);

            if (varIntSizedAuth) {
                CodecUtils.writeVarIntSizedBytes(buf, authentication);
            } else {
                buf.writeByte(authentication.length).writeBytes(authentication);
            }

            if (!database.isEmpty()) {
                CodecUtils.writeCString(buf, database, charset);
            }

            if (authType != null) {
                CodecUtils.writeCString(buf, authType.getNativeName(), charset);
            }

            return writeAttrsWithRetained(buf, charset);
        } finally {
            buf.release();
        }
    }

    private ByteBuf writeAttrsWithRetained(ByteBuf buf, Charset charset) {
        if (attributes.isEmpty()) { // no need write attributes
            return buf.retain();
        }

        final ByteBuf attributesBuf = buf.alloc().buffer();

        try {
            // attributesBuf write first
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                CodecUtils.writeVarIntSizedString(attributesBuf, entry.getKey(), charset);
                CodecUtils.writeVarIntSizedString(attributesBuf, entry.getValue(), charset);
            }

            // write attributesBuf to buf after write attributesBuf size by var int encoded
            CodecUtils.writeVarInt(buf, attributesBuf.readableBytes());

            CompositeByteBuf finalBuf = buf.alloc().compositeBuffer(2);

            try {
                return finalBuf.addComponent(true, buf.retain())
                    .addComponent(true, attributesBuf.retain())
                    .retain();
            } finally {
                finalBuf.release();
            }
        } finally {
            attributesBuf.release();
        }
    }

    private int calculateCapabilities(int capabilities) {
        if (database.isEmpty()) {
            capabilities &= ~Capability.CONNECT_WITH_DB.getFlag();
        } else {
            capabilities |= Capability.CONNECT_WITH_DB.getFlag();
        }

        if (attributes.isEmpty()) {
            capabilities &= ~Capability.CONNECT_ATTRS.getFlag();
        } else {
            capabilities |= Capability.CONNECT_ATTRS.getFlag();
        }

        return capabilities;
    }
}
