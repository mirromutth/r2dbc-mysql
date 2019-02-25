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

package io.github.mirromutth.r2dbc.mysql.message.frontend;

import io.github.mirromutth.r2dbc.mysql.core.ServerSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import reactor.util.annotation.Nullable;

/**
 * The request message tells the MySQL client to exit.
 */
public final class ExitMessage extends AbstractFrontendMessage {

    private static final ExitMessage INSTANCE = new ExitMessage();

    private ExitMessage() {
    }

    public static ExitMessage getInstance() {
        return INSTANCE;
    }

    @Override
    protected ByteBuf encodeSingle(ByteBufAllocator bufAllocator, @Nullable ServerSession session) {
        return Unpooled.wrappedBuffer(new byte[] { 0x01 });
    }

    @Override
    public String toString() {
        return "ExitMessage{}";
    }
}
