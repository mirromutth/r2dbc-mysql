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

import dev.miku.r2dbc.mysql.Capability;
import dev.miku.r2dbc.mysql.ConnectionContext;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Arrays;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A handshake response message for protocol version 3.20.
 *
 * @see SslRequest320 the header of {@link HandshakeResponse320}.
 */
final class HandshakeResponse320 extends ScalarClientMessage implements HandshakeResponse {

    private final SslRequest320 header;

    private final String user;

    private final byte[] authentication;

    private final String database;

    HandshakeResponse320(int envelopeId, Capability capability, String user, byte[] authentication, String database) {
        this.header = new SslRequest320(envelopeId, capability);
        this.user = requireNonNull(user, "user must not be null");
        this.authentication = requireNonNull(authentication, "authentication must not be null");
        this.database = requireNonNull(database, "database must not be null");
    }

    @Override
    public int getEnvelopeId() {
        return header.getEnvelopeId();
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

        return header.equals(that.header) && user.equals(that.user) &&
            Arrays.equals(authentication, that.authentication) && database.equals(that.database);
    }

    @Override
    public int hashCode() {
        int result = header.hashCode();
        result = 31 * result + user.hashCode();
        result = 31 * result + Arrays.hashCode(authentication);
        return 31 * result + database.hashCode();
    }

    @Override
    public String toString() {
        return "HandshakeResponse320{envelopeId=" + header.getEnvelopeId() +
            ", capability=" + header.getCapability() + ", user='" + user +
            "', authentication=REDACTED, database='" + database + "'}";
    }

    @Override
    protected void writeTo(ByteBuf buf, ConnectionContext context) {
        header.writeTo(buf);

        Charset charset = context.getClientCollation().getCharset();

        HandshakeResponse.writeCString(buf, user, charset);

        if (header.getCapability().isConnectWithDatabase()) {
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
        } else {
            // Write to end-of-buffer because has no database following.
            buf.writeBytes(authentication);
        }
    }
}
