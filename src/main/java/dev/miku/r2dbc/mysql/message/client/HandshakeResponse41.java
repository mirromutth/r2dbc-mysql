/*
 * Copyright 2018-2020 the original author or authors.
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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A handshake response message sent by clients those supporting
 * {@link Capabilities#PROTOCOL_41} if the server announced it in
 * it's {@code HandshakeV10Message}, otherwise sending to an old
 * server should use the {@link HandshakeResponse320}.
 * <p>
 * Should make sure {@code clientCapabilities} is right before
 * construct this instance, i.e. {@link Capabilities#CONNECT_ATTRS},
 * {@link Capabilities#CONNECT_WITH_DB} or other capabilities.
 *
 * @see SslRequest41 the head of {@link HandshakeResponse41}.
 */
final class HandshakeResponse41 extends ScalarClientMessage implements HandshakeResponse {

    private static final int ONE_BYTE_MAX_INT = 0xFF;

    private final SslRequest41 head;

    private final String user;

    private final byte[] authentication;

    private final String authType;

    private final String database;

    private final Map<String, String> attributes;

    // private final byte zStdCompressionLevel; // When Z-Standard compression supporting

    HandshakeResponse41(int envelopeId, int capabilities, int collationId, String user, byte[] authentication, String authType, String database, Map<String, String> attributes) {
        this.head = new SslRequest41(envelopeId, capabilities, collationId);
        this.user = requireNonNull(user, "user must not be null");
        this.authentication = requireNonNull(authentication, "authentication must not be null");
        this.database = requireNonNull(database, "database must not be null");
        this.authType = requireNonNull(authType, "authType must not be null");
        this.attributes = requireNonNull(attributes, "attributes must not be null");
    }

    @Override
    public int getEnvelopeId() {
        return head.getEnvelopeId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HandshakeResponse41 that = (HandshakeResponse41) o;

        return head.equals(that.head) && user.equals(that.user) &&
            Arrays.equals(authentication, that.authentication) && authType.equals(that.authType) &&
            database.equals(that.database) && attributes.equals(that.attributes);
    }

    @Override
    public int hashCode() {
        int result = head.hashCode();
        result = 31 * result + user.hashCode();
        result = 31 * result + Arrays.hashCode(authentication);
        result = 31 * result + authType.hashCode();
        result = 31 * result + database.hashCode();
        return 31 * result + attributes.hashCode();
    }

    @Override
    public String toString() {
        return "HandshakeResponse41{envelopeId=" + head.getEnvelopeId() +
            ", capabilities=" + Integer.toHexString(head.getCapabilities()) +
            ", collationId=" + head.getCollationId() + ", user='" + user +
            "', authentication=REDACTED, authType='" + authType +
            "', database='" + database + "', attributes=" + attributes + '}';
    }

    @Override
    protected void writeTo(ByteBuf buf, ConnectionContext context) {
        head.writeTo(buf);

        int capabilities = head.getCapabilities();
        Charset charset = context.getClientCollation().getCharset();

        HandshakeResponse.writeCString(buf, user, charset);

        if ((capabilities & Capabilities.PLUGIN_AUTH_VAR_INT_SIZED_DATA) != 0) {
            writeVarIntSizedBytes(buf, authentication);
        } else if (authentication.length <= ONE_BYTE_MAX_INT) {
            buf.writeByte(authentication.length).writeBytes(authentication);
        } else {
            // Auth change message will be sent by server.
            buf.writeByte(0);
        }

        if ((capabilities & Capabilities.CONNECT_WITH_DB) != 0) {
            HandshakeResponse.writeCString(buf, database, charset);
        }

        if ((capabilities & Capabilities.PLUGIN_AUTH) != 0) {
            // This must be an UTF-8 string.
            HandshakeResponse.writeCString(buf, authType, StandardCharsets.UTF_8);
        }

        if ((capabilities & Capabilities.CONNECT_ATTRS) != 0) {
            writeAttrs(buf, charset);
        }
    }

    private void writeAttrs(ByteBuf buf, Charset charset) {
        if (attributes.isEmpty()) {
            // It is zero of var int, not terminal.
            buf.writeByte(0);
            return;
        }

        final ByteBuf attributesBuf = buf.alloc().buffer();

        try {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                writeVarIntString(attributesBuf, entry.getKey(), charset);
                writeVarIntString(attributesBuf, entry.getValue(), charset);
            }

            writeVarIntSizedBytes(buf, attributesBuf);
        } finally {
            attributesBuf.release();
        }
    }

    private static void writeVarIntString(ByteBuf buf, String value, Charset charset) {
        if (value.isEmpty()) {
            // It is zero of var int, not terminal.
            buf.writeByte(0);
            return;
        }

        // NEVER use value.length() in here, size must be bytes' size, not string size.
        // Can not use reserved var integer, because this buffer header has been used.
        writeVarIntSizedBytes(buf, value.getBytes(charset));
    }

    private static void writeVarIntSizedBytes(ByteBuf buf, byte[] value) {
        int size = value.length;

        if (size == 0) {
            // It is zero of var int, not terminal.
            buf.writeByte(0);
            return;
        }

        VarIntUtils.writeVarInt(buf, size);
        buf.writeBytes(value);
    }

    private static void writeVarIntSizedBytes(ByteBuf buf, ByteBuf value) {
        int size = value.readableBytes();

        if (size == 0) {
            // It is zero of var int, not terminal.
            buf.writeByte(0);
            return;
        }

        VarIntUtils.writeVarInt(buf, size);
        buf.writeBytes(value);
    }
}
