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
import io.github.mirromutth.r2dbc.mysql.constant.ProtocolConstants;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.nio.charset.Charset;
import java.util.Map;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A handshake response message sent by clients those supporting
 * {@link Capability#PROTOCOL_41} if the server announced it in
 * it's {@code HandshakeV10Message}, otherwise talking to an old
 * server should use the handshake 320 response message, but
 * protocol 320 should be deprecated on MySQL 5.x.
 * <p>
 * Should make sure {@link #clientCapabilities} is right before construct this instance,
 * e.g. {@link Capability#CONNECT_ATTRS}, {@link Capability#CONNECT_WITH_DB} or other capabilities.
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
        String database,
        Map<String, String> attributes
    ) {
        this.clientCapabilities = clientCapabilities;
        this.collationLow8Bits = collationLow8Bits;
        this.username = requireNonNull(username, "username must not be null");
        this.authentication = requireNonNull(authentication, "authentication must not be null");
        this.database = requireNonNull(database, "database must not be null");
        this.authType = requireNonNull(authType, "authType must not be null");
        this.attributes = requireNonNull(attributes, "attributes must not be null");
        this.varIntSizedAuth = (clientCapabilities & Capability.PLUGIN_AUTH_VAR_INT_SIZED_DATA.getFlag()) != 0;

        // authentication can not longer than 255 if server is not support use var int encode authentication
        if (!this.varIntSizedAuth && authentication.length > ONE_BYTE_MAX_INT) {
            throw new IllegalArgumentException("authentication too long, server not support size " + authentication.length);
        }
    }

    @Override
    protected ByteBuf encodeSingle(ByteBufAllocator bufAllocator, MySqlSession session) {
        Charset charset = session.getCollation().getCharset();
        final ByteBuf buf = bufAllocator.buffer();

        try {
            buf.writeIntLE(clientCapabilities)
                .writeIntLE(ProtocolConstants.MAX_PART_SIZE + 1) // 16777216, means include sequence id or exclusive logic in MySQL.
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

            return writeAttrs(buf, charset);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    private ByteBuf writeAttrs(ByteBuf buf, Charset charset) {
        if (attributes.isEmpty()) { // no need write attributes
            return buf;
        }

        final ByteBuf attributesBuf = buf.alloc().buffer();

        try {
            // attributesBuf write first
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                CodecUtils.writeVarIntSizedString(attributesBuf, entry.getKey(), charset);
                CodecUtils.writeVarIntSizedString(attributesBuf, entry.getValue(), charset);
            }

            CodecUtils.writeVarIntSizedBytes(buf, attributesBuf);

            return buf;
        } finally {
            attributesBuf.release();
        }
    }
}
