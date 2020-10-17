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
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Arrays;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A handshake response message sent by clients those do not supporting
 * {@link Capabilities#PROTOCOL_41} if the server announced it in
 * it's {@code HandshakeV10Message}, otherwise sending to an old
 * server should use the {@link HandshakeResponse41}.
 * <p>
 * Should make sure {@code clientCapabilities} is right before
 * construct this instance, i.e. {@link Capabilities#CONNECT_WITH_DB}.
 *
 * @see SslRequest320 the head of {@link HandshakeResponse320}.
 */
final class HandshakeResponse320 extends ScalarClientMessage implements HandshakeResponse {

    private final SslRequest320 head;

    private final String user;

    private final byte[] authentication;

    private final String database;

    HandshakeResponse320(int envelopeId, int capabilities, String user, byte[] authentication, String database) {
        this.head = new SslRequest320(envelopeId, capabilities);
        this.user = requireNonNull(user, "user must not be null");
        this.authentication = requireNonNull(authentication, "authentication must not be null");
        this.database = requireNonNull(database, "database must not be null");
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

        HandshakeResponse320 that = (HandshakeResponse320) o;

        return head.equals(that.head) && user.equals(that.user) &&
            Arrays.equals(authentication, that.authentication) && database.equals(that.database);
    }

    @Override
    public int hashCode() {
        int result = head.hashCode();
        result = 31 * result + user.hashCode();
        result = 31 * result + Arrays.hashCode(authentication);
        return 31 * result + database.hashCode();
    }

    @Override
    public String toString() {
        return "HandshakeResponse320{envelopeId=" + head.getEnvelopeId() +
            ", capabilities=" + Integer.toHexString(head.getCapabilities()) + ", user='" + user +
            "', authentication=REDACTED, database='" + database + "'}";
    }

    @Override
    protected void writeTo(ByteBuf buf, ConnectionContext context) {
        head.writeTo(buf);

        Charset charset = context.getClientCollation().getCharset();

        HandshakeResponse.writeCString(buf, user, charset);

        if ((head.getCapabilities() & Capabilities.CONNECT_WITH_DB) == 0) {
            // Write to end-of-buffer because has no database following.
            buf.writeBytes(authentication);
        } else {
            if (authentication.length == 0) {
                buf.writeByte(TERMINAL);
            } else {
                buf.writeBytes(authentication);

                if (authentication[authentication.length - 1] != TERMINAL) {
                    // Should not write twice terminal.
                    buf.writeByte(TERMINAL);
                }
            }

            HandshakeResponse.writeCString(buf, database, charset);
        }
    }
}
