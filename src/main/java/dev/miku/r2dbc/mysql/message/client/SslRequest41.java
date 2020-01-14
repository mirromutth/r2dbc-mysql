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

import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.constant.Envelopes;
import io.netty.buffer.ByteBuf;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * The ssl request message on protocol 4.1. It is also first part of {@link HandshakeResponse41}.
 */
final class SslRequest41 extends FixedSizeClientMessage implements SslRequest {

    private static final int FILTER_SIZE = 23;

    private static final int BUF_SIZE = Integer.BYTES + Integer.BYTES + Byte.BYTES + FILTER_SIZE;

    private final int capabilities;

    private final int collationId;

    /**
     * @param capabilities client capabilities, see {@link Capabilities}
     * @param collationId  0 if server not support protocol 41 or has been not give collation
     */
    SslRequest41(int capabilities, int collationId) {
        require(collationId > 0, "collationId must be a positive integer");

        this.capabilities = capabilities;
        this.collationId = collationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SslRequest41)) {
            return false;
        }

        SslRequest41 that = (SslRequest41) o;

        if (capabilities != that.capabilities) {
            return false;
        }
        return collationId == that.collationId;
    }

    @Override
    public int hashCode() {
        int result = capabilities;
        result = 31 * result + collationId;
        return result;
    }

    @Override
    public String toString() {
        return String.format("SslRequest41{capabilities=%x, collationId=%d}", capabilities, collationId);
    }

    @Override
    public int getCapabilities() {
        return capabilities;
    }

    @Override
    protected int size() {
        return BUF_SIZE;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeIntLE(capabilities)
            .writeIntLE(Envelopes.MAX_ENVELOPE_SIZE)
            .writeByte(collationId & 0xFF) // only low 8-bits
            .writeZero(FILTER_SIZE);
    }

    int getCollationId() {
        return collationId;
    }
}
