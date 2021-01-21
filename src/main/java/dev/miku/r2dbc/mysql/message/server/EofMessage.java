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

/**
 * Base message considers EOF.
 * <p>
 * Note: EOF message are deprecated and OK message are also used to indicate EOF as of MySQL 5.7.5.
 */
public interface EofMessage extends CompleteMessage {

    /**
     * Decode EOF message by a {@link ByteBuf}, automatically identify protocol version.
     *
     * @param buf the {@link ByteBuf}.
     * @return decoded EOF message.
     */
    static EofMessage decode(ByteBuf buf) {
        if (buf.readableBytes() >= Eof41Message.SIZE) {
            return Eof41Message.decode(buf);
        }

        return Eof320Message.INSTANCE;
    }

    /**
     * Check whether the length of buffer is the valid length for decoding the EOF message.
     *
     * @param readableBytes the length of buffer.
     * @return if the length valid.
     */
    static boolean isValidSize(int readableBytes) {
        return readableBytes == Eof320Message.SIZE || readableBytes == Eof41Message.SIZE;
    }
}
