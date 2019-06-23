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
import io.github.mirromutth.r2dbc.mysql.constant.Envelopes;
import io.netty.buffer.ByteBuf;

/**
 * The ssl request message.
 * <p>
 * Note: protocol 41 ALWAYS be used.
 */
public final class SslRequestMessage extends FixedSizeClientMessage implements ExchangeableMessage {

    private static final int FILTER_SIZE = 23;

    private static final int BUF_SIZE = Integer.BYTES + Integer.BYTES + Byte.BYTES + FILTER_SIZE;

    private final int clientCapabilities;

    private final int collationId;

    /**
     * @param clientCapabilities client capabilities, see {@link Capabilities}
     * @param collationId  0 if server not support protocol 41 or has been not give collation
     */
    public SslRequestMessage(int clientCapabilities, int collationId) {
        this.clientCapabilities = clientCapabilities;
        this.collationId = collationId;
    }

    @Override
    public boolean isSequenceIdReset() {
        return false;
    }

    @Override
    protected int size() {
        return BUF_SIZE;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeIntLE(clientCapabilities)
            .writeIntLE(Envelopes.MAX_ENVELOPE_SIZE)
            .writeByte(collationId) // only low 8-bits.
            .writeZero(FILTER_SIZE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SslRequestMessage)) {
            return false;
        }

        SslRequestMessage that = (SslRequestMessage) o;

        if (clientCapabilities != that.clientCapabilities) {
            return false;
        }
        return collationId == that.collationId;

    }

    @Override
    public int hashCode() {
        int result = clientCapabilities;
        result = 31 * result + collationId;
        return result;
    }

    @Override
    public String toString() {
        return "SslRequestMessage{" +
            "clientCapabilities=" + clientCapabilities +
            ", collationId=" + collationId +
            '}';
    }
}
