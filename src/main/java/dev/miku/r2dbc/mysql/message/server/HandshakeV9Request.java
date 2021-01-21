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

package dev.miku.r2dbc.mysql.message.server;

import dev.miku.r2dbc.mysql.Capability;
import dev.miku.r2dbc.mysql.authentication.MySqlAuthProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.util.Arrays;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_BYTES;

/**
 * MySQL Handshake Message for handshake protocol version 9.
 */
final class HandshakeV9Request implements HandshakeRequest {

    private static final Capability SERVER_CAPABILITY = Capability.of(0);

    private final HandshakeHeader header;

    private final int envelopeId;

    private final byte[] salt;

    private HandshakeV9Request(HandshakeHeader header, int envelopeId, byte[] salt) {
        this.header = requireNonNull(header, "header must not be null");
        this.envelopeId = envelopeId;
        this.salt = requireNonNull(salt, "salt must not be null");
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
    public Capability getServerCapability() {
        return SERVER_CAPABILITY;
    }

    @Override
    public String getAuthType() {
        return MySqlAuthProvider.MYSQL_OLD_PASSWORD;
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HandshakeV9Request that = (HandshakeV9Request) o;

        return envelopeId == that.envelopeId && header.equals(that.header) && Arrays.equals(salt, that.salt);
    }

    @Override
    public int hashCode() {
        int hash = 31 * header.hashCode() + envelopeId;
        return 31 * hash + Arrays.hashCode(salt);
    }

    @Override
    public String toString() {
        return "HandshakeV9Request{header=" + header + ", envelopeId=" + envelopeId + ", salt=REDACTED}";
    }

    static HandshakeV9Request decode(int envelopeId, ByteBuf buf, HandshakeHeader header) {
        int bytes = buf.readableBytes();

        if (bytes <= 0) {
            return new HandshakeV9Request(header, envelopeId, EMPTY_BYTES);
        }

        byte[] salt;

        if (buf.getByte(buf.writerIndex() - 1) == TERMINAL) {
            salt = ByteBufUtil.getBytes(buf, buf.readerIndex(), bytes - 1);
        } else {
            salt = ByteBufUtil.getBytes(buf);
        }

        return new HandshakeV9Request(header, envelopeId, salt);
    }
}
