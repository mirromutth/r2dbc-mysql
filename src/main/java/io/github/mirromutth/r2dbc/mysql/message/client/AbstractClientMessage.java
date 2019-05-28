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

import io.github.mirromutth.r2dbc.mysql.constant.Envelopes;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.header.SequenceIdProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import reactor.core.publisher.Flux;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Every client message need slice huge package to multi-envelopes.
 */
abstract class AbstractClientMessage implements ClientMessage {

    /**
     * Encode to single {@link ByteBuf} without packet header
     *
     * @param bufAllocator {@link ByteBuf} allocator that from netty connection usually.
     * @param session      current MySQL session.
     * @return single {@link ByteBuf}, can not be null, maybe read only.
     */
    protected abstract ByteBuf encodeSingle(ByteBufAllocator bufAllocator, MySqlSession session);

    protected abstract boolean resetSequenceId();

    @Override
    public final void writeAndFlush(ChannelHandlerContext ctx, SequenceIdProvider sequenceIdProvider, MySqlSession session) {
        requireNonNull(ctx, "ctx must not be null");
        requireNonNull(sequenceIdProvider, "sequenceIdProvider must not be null");

        ByteBufAllocator allocator = ctx.alloc();
        ByteBuf allBodyBuf = encodeSingle(allocator, session); // maybe read only buffer

        if (resetSequenceId()) {
            sequenceIdProvider.reset();
        }

        while (allBodyBuf.readableBytes() >= Envelopes.MAX_PART_SIZE) {
            ByteBuf headerBuf = allocator.buffer(Envelopes.PART_HEADER_SIZE, Envelopes.PART_HEADER_SIZE)
                .writeMediumLE(Envelopes.MAX_PART_SIZE)
                .writeByte(sequenceIdProvider.next());

            ctx.write(headerBuf);
            ctx.write(allBodyBuf.readRetainedSlice(Envelopes.MAX_PART_SIZE));
        }

        // Should write a empty envelope to server even body is empty. (like complete frame)
        ByteBuf headerBuf = allocator.buffer(Envelopes.PART_HEADER_SIZE, Envelopes.PART_HEADER_SIZE)
            .writeMediumLE(allBodyBuf.readableBytes())
            .writeByte(sequenceIdProvider.next());

        ctx.write(headerBuf);
        ctx.writeAndFlush(allBodyBuf);
    }
}
