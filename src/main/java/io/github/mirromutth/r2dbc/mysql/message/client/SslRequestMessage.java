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
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * The ssl request message.
 */
public final class SslRequestMessage extends AbstractClientMessage {

    private static final int FILTER_SIZE = 23;

    private final int clientCapabilities;

    private final byte collationLow8Bits;

    /**
     * @param clientCapabilities client capabilities, see {@link Capabilities}
     * @param collationLow8Bits 0 if server not support protocol 41 or has been not give collation
     */
    public SslRequestMessage(int clientCapabilities, byte collationLow8Bits) {
        this.clientCapabilities = clientCapabilities;

        if ((clientCapabilities & Capabilities.PROTOCOL_41) != 0) {
            this.collationLow8Bits = collationLow8Bits;
        } else {
            this.collationLow8Bits = 0;
        }
    }

    @Override
    public boolean isSequenceIdReset() {
        return false;
    }

    @Override
    protected ByteBuf encodeSingle(ByteBufAllocator bufAllocator, MySqlSession session) {
        final ByteBuf buf = bufAllocator.buffer();

        try {
            if ((clientCapabilities & Capabilities.PROTOCOL_41) != 0) {
                buf.writeIntLE(clientCapabilities)
                    .writeIntLE(Envelopes.MAX_PART_SIZE)
                    .writeByte(collationLow8Bits)
                    .writeZero(FILTER_SIZE);
            } else {
                buf.writeShortLE(clientCapabilities & 0xFFFF).writeMediumLE(Envelopes.MAX_PART_SIZE);
            }

            return buf;
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }
}
