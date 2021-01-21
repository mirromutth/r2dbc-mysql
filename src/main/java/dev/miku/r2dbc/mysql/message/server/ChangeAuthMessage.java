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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.util.Arrays;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Change authentication plugin type and salt message.
 */
public final class ChangeAuthMessage implements ServerMessage {

    private final int envelopeId;

    private final String authType;

    private final byte[] salt;

    private ChangeAuthMessage(int envelopeId, String authType, byte[] salt) {
        this.envelopeId = envelopeId;
        this.authType = requireNonNull(authType, "authType must not be null");
        this.salt = requireNonNull(salt, "salt must not be null");
    }

    public int getEnvelopeId() {
        return envelopeId;
    }

    public String getAuthType() {
        return authType;
    }

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

        ChangeAuthMessage that = (ChangeAuthMessage) o;

        return envelopeId == that.envelopeId && authType.equals(that.authType) &&
            Arrays.equals(salt, that.salt);
    }

    @Override
    public int hashCode() {
        int result = envelopeId;
        result = 31 * result + authType.hashCode();
        return 31 * result + Arrays.hashCode(salt);
    }

    @Override
    public String toString() {
        return "ChangeAuthMessage{envelopeId=" + envelopeId + ", authType='" + authType + "', salt=REDACTED}";
    }

    static ChangeAuthMessage decode(int envelopeId, ByteBuf buf) {
        buf.skipBytes(1); // skip generic header 0xFE of change authentication messages

        String authType = HandshakeHeader.readCStringAscii(buf);
        int bytes = buf.readableBytes();
        byte[] salt = bytes > 0 && buf.getByte(buf.writerIndex() - 1) == TERMINAL ?
            ByteBufUtil.getBytes(buf, buf.readerIndex(), bytes - 1) :
            ByteBufUtil.getBytes(buf);

        // The terminal character has been removed from salt.
        return new ChangeAuthMessage(envelopeId, authType, salt);
    }
}
