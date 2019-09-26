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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;

/**
 * A message sent from a MySQL client to a MySQL server.
 */
public interface ClientMessage {

    /**
     * Encode a message into a {@link ByteBuf} data buffer without envelope header.
     *
     * @param allocator the {@link ByteBufAllocator} to use to get a {@link ByteBuf} data buffer to write into.
     * @param context   current MySQL connection context
     */
    Publisher<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context);
}
