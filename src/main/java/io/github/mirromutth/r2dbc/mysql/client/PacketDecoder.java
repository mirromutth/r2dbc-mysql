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

package io.github.mirromutth.r2dbc.mysql.client;

import io.github.mirromutth.r2dbc.mysql.constant.ProtocolConstants;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.ByteOrder;

/**
 * Decode native message packet of MySQL.
 */
final class PacketDecoder extends LengthFieldBasedFrameDecoder {

    PacketDecoder() {
        super(
            ByteOrder.LITTLE_ENDIAN,
            ProtocolConstants.MAX_PART_SIZE + ProtocolConstants.PART_HEADER_SIZE,
            0,
            ProtocolConstants.SIZE_FIELD_SIZE,
            1, // byte size of sequence Id field
            0, // do NOT strip header
            true
        );
    }

    /**
     * Override this method because {@code ByteBuf.order(order)} is likely to create temporary
     * {@code SwappedByteBuf}, and {@code ByteBuf.order(order)} has also been deprecated.
     *
     * {@inheritDoc}
     */
    @Override
    protected long getUnadjustedFrameLength(ByteBuf buf, int offset, int length, ByteOrder order) {
        if (length != ProtocolConstants.SIZE_FIELD_SIZE || order != ByteOrder.LITTLE_ENDIAN) {
            // impossible length or order, only BUG or hack of reflect
            throw new DecoderException("unsupported lengthFieldLength: " + length + " (only 3) or byteOrder: " + order + " (only LITTLE_ENDIAN)");
        }

        return buf.getUnsignedMediumLE(offset);
    }
}
