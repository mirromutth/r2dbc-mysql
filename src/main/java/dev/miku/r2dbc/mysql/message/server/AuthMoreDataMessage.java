/*
 * Copyright 2018-2021 the original author or authors.
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

/**
 * Authentication more data request, means continue send auth change response message if is exists.
 */
public final class AuthMoreDataMessage implements ServerMessage {

    private static final byte AUTH_SUCCEED = 3;

    private final int envelopeId;

    private final boolean failed;

    private AuthMoreDataMessage(int envelopeId, boolean failed) {
        this.envelopeId = envelopeId;
        this.failed = failed;
    }

    public int getEnvelopeId() {
        return envelopeId;
    }

    public boolean isFailed() {
        return failed;
    }

    static AuthMoreDataMessage decode(int envelopeId, ByteBuf buf) {
        buf.skipBytes(1); // auth more data message header, 0x01

        return new AuthMoreDataMessage(envelopeId, buf.readByte() != AUTH_SUCCEED);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AuthMoreDataMessage that = (AuthMoreDataMessage) o;

        return envelopeId == that.envelopeId && failed == that.failed;
    }

    @Override
    public int hashCode() {
        return (envelopeId << 1) | (failed ? 1 : 0);
    }

    @Override
    public String toString() {
        return "AuthMoreDataMessage{envelopeId=" + envelopeId + ", failed=" + failed + '}';
    }
}
