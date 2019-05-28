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

import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

/**
 * The request message check alive of MySQL server.
 */
public final class PingMessage extends AbstractClientMessage implements ExchangeableMessage {

    private static final PingMessage INSTANCE = new PingMessage();

    private PingMessage() {
    }

    public static PingMessage getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean resetSequenceId() {
        return true;
    }

    @Override
    protected ByteBuf encodeSingle(ByteBufAllocator bufAllocator, MySqlSession session) {
        return Unpooled.wrappedBuffer(new byte[] { 0x0E });
    }

    @Override
    public String toString() {
        return "PingMessage{}";
    }
}
