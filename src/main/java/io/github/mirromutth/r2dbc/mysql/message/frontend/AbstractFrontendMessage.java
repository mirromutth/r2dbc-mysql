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

import io.github.mirromutth.r2dbc.mysql.constant.ProtocolConstants;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Every frontend message need slice huge package to multi-envelopes.
 */
abstract class AbstractFrontendMessage implements FrontendMessage {

    /**
     * Encode to single {@link ByteBuf} without packet header
     *
     * @param bufAllocator {@link ByteBuf} allocator that from netty connection usually.
     * @param session      current MySQL session.
     * @return single {@link ByteBuf}, can not be null.
     */
    protected abstract ByteBuf encodeSingle(ByteBufAllocator bufAllocator, @Nullable MySqlSession session);

    @Override
    public final Flux<ByteBuf> encode(ByteBufAllocator bufAllocator, AtomicInteger sequenceId, @Nullable MySqlSession session) {
        requireNonNull(bufAllocator, "bufAllocator must not be null");
        requireNonNull(sequenceId, "sequenceId must not be null");

        ByteBuf allBodyBuf = encodeSingle(bufAllocator, session);
        List<ByteBuf> envelopes = new ArrayList<>(allBodyBuf.readableBytes() / ProtocolConstants.MAX_PART_SIZE + 1);

        while (allBodyBuf.readableBytes() >= ProtocolConstants.MAX_PART_SIZE) {
            ByteBuf headerBuf = bufAllocator.buffer(4).writeMediumLE(ProtocolConstants.MAX_PART_SIZE).writeByte(sequenceId.getAndIncrement());
            envelopes.add(Unpooled.wrappedBuffer(headerBuf, allBodyBuf.readRetainedSlice(ProtocolConstants.MAX_PART_SIZE)));
        }

        ByteBuf headerBuf = bufAllocator.buffer(4).writeMediumLE(allBodyBuf.readableBytes()).writeByte(sequenceId.getAndIncrement());
        envelopes.add(Unpooled.wrappedBuffer(headerBuf, allBodyBuf));

        return Flux.fromIterable(envelopes);
    }
}
