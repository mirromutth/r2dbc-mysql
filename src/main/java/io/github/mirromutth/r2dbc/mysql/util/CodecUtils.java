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

package io.github.mirromutth.r2dbc.mysql.util;

import io.github.mirromutth.r2dbc.mysql.exception.TerminateNotFoundException;
import io.github.mirromutth.r2dbc.mysql.message.PacketHeader;
import io.netty.buffer.ByteBuf;

import static io.github.mirromutth.r2dbc.mysql.constant.Packets.TERMINAL;
import static java.util.Objects.requireNonNull;

/**
 * Common codec methods util.
 */
public final class CodecUtils {

    private CodecUtils() {
    }

    /**
     * @param buf C-style string readable buffer
     * @return The string of C-style, it may just be a byte array,
     * should NEVER convert it to real string, should release manually.
     */
    public static ByteBuf readCString(ByteBuf buf) {
        requireNonNull(buf, "buf must not be null");

        int size = buf.bytesBefore(TERMINAL);

        if (size < 0) {
            throw new TerminateNotFoundException();
        }

        ByteBuf result = buf.readBytes(size);
        buf.skipBytes(1); // ignore last byte that is TERMINAL.
        return result;
    }

    public static PacketHeader readPacketHeader(ByteBuf buf) {
        int byteSize = buf.readUnsignedMediumLE();
        return new PacketHeader(byteSize, buf.readUnsignedByte());
    }
}
