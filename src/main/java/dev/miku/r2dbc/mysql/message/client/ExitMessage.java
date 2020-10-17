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

import io.netty.buffer.ByteBuf;

/**
 * The request message tells the MySQL client to exit.
 */
public final class ExitMessage extends SizedClientMessage {

    private static final int EXIT_FLAG = 0x01;

    public static final ExitMessage INSTANCE = new ExitMessage();

    @Override
    protected int size() {
        return Byte.BYTES;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeByte(EXIT_FLAG);
    }

    @Override
    public String toString() {
        return "ExitMessage{}";
    }

    private ExitMessage() {
    }
}
