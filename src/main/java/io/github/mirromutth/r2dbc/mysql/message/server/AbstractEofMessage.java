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

package io.github.mirromutth.r2dbc.mysql.message.server;

import io.netty.buffer.ByteBuf;

/**
 * Base message considers EOF.
 * <p>
 * Note: EOF message are deprecated and OK message are also used to indicate EOF as of MySQL 5.7.5.
 */
public abstract class AbstractEofMessage implements ServerMessage {

    static AbstractEofMessage decode(ByteBuf buf) {
        if (buf.readableBytes() >= Eof41Message.SIZE) {
            return Eof41Message.decode(buf);
        } else {
            return Eof320Message.INSTANCE;
        }
    }

    static boolean isValidSize(int readableBytes) {
        return readableBytes == Eof320Message.SIZE || readableBytes == Eof41Message.SIZE;
    }
}
