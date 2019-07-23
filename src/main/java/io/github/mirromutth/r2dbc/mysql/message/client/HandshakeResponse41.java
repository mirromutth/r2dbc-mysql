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

package io.github.mirromutth.r2dbc.mysql.message.client;

import io.github.mirromutth.r2dbc.mysql.constant.Capabilities;
import io.github.mirromutth.r2dbc.mysql.internal.CodecUtils;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static io.github.mirromutth.r2dbc.mysql.constant.EmptyArrays.EMPTY_BYTES;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

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
final class HandshakeResponse41 extends EnvelopeClientMessage implements HandshakeResponse {

    private static final int ONE_BYTE_MAX_INT = 0xFF;

    private final SslRequest41 head;

    private final String username;

    private final byte[] authentication;

    private final String authType;

    private final String database;

    private final Map<String, String> attributes;

    HandshakeResponse41(int capabilities, int collationId, String username, byte[] authentication, String authType, String database, Map<String, String> attributes) {
        this.head = new SslRequest41(capabilities, collationId);

        this.username = requireNonNull(username, "username must not be null");
        this.authentication = requireNonNull(authentication, "authentication must not be null");
        this.database = requireNonNull(database, "database must not be null");
        this.authType = requireNonNull(authType, "authType must not be null");
        this.attributes = requireNonNull(attributes, "attributes must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HandshakeResponse41)) {
            return false;
        }

        HandshakeResponse41 that = (HandshakeResponse41) o;

        if (!head.equals(that.head)) {
            return false;
        }
        if (!username.equals(that.username)) {
            return false;
        }
        if (!Arrays.equals(authentication, that.authentication)) {
            return false;
        }
        if (!authType.equals(that.authType)) {
            return false;
        }
        if (!database.equals(that.database)) {
            return false;
        }
        return attributes.equals(that.attributes);
    }

    @Override
    public int hashCode() {
        int result = head.hashCode();
        result = 31 * result + username.hashCode();
        result = 31 * result + Arrays.hashCode(authentication);
        result = 31 * result + authType.hashCode();
        result = 31 * result + database.hashCode();
        result = 31 * result + attributes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("HandshakeResponse41{capabilities=%x, collationId=%d, username='%s', authentication=REDACTED, authType='%s', database='%s', attributes=%s}",
            head.getCapabilities(), head.getCollationId(), username, authType, database, attributes);
    }

    @Override
    protected void writeTo(ByteBuf buf, MySqlSession session) {
        head.writeTo(buf);

        int capabilities = head.getCapabilities();
        Charset charset = session.getCollation().getCharset();

        CodecUtils.writeCString(buf, username, charset);

        if ((capabilities & Capabilities.PLUGIN_AUTH_VAR_INT_SIZED_DATA) != 0) {
            CodecUtils.writeVarIntSizedBytes(buf, authentication);
        } else if (authentication.length <= ONE_BYTE_MAX_INT) {
            buf.writeByte(authentication.length).writeBytes(authentication);
        } else {
            // Auth change message will be sent by server.
            buf.writeByte(0);
        }

        if ((capabilities & Capabilities.CONNECT_WITH_DB) != 0) {
            CodecUtils.writeCString(buf, database, charset);
        }

        if ((capabilities & Capabilities.PLUGIN_AUTH) != 0) {
            // This must be an UTF-8 string.
            CodecUtils.writeCString(buf, authType, StandardCharsets.UTF_8);
        }

        if ((capabilities & Capabilities.CONNECT_ATTRS) != 0) {
            writeAttrs(buf, charset);
        }
    }

    private void writeAttrs(ByteBuf buf, Charset charset) {
        if (attributes.isEmpty()) {
            CodecUtils.writeVarIntSizedBytes(buf, EMPTY_BYTES);
            return;
        }

        final ByteBuf attributesBuf = buf.alloc().buffer();

        try {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                CodecUtils.writeVarIntSizedString(attributesBuf, entry.getKey(), charset);
                CodecUtils.writeVarIntSizedString(attributesBuf, entry.getValue(), charset);
            }

            CodecUtils.writeVarIntSizedBytes(buf, attributesBuf);
        } finally {
            attributesBuf.release();
        }
    }
}
