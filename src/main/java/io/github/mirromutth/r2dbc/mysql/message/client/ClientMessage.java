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
import io.github.mirromutth.r2dbc.mysql.message.header.SequenceIdProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import reactor.core.publisher.Flux;

/**
 * A message sent from a MySQL client to a MySQL server.
 */
public interface ClientMessage {

    /**
     * Write and flush encoded {@link ByteBuf} to netty channel.
     *
     * @param ctx                connection's channel context
     * @param sequenceIdProvider message sequence id provider
     * @param session            current MySQL session
     */
    void writeAndFlush(ChannelHandlerContext ctx, SequenceIdProvider sequenceIdProvider, MySqlSession session);
}
