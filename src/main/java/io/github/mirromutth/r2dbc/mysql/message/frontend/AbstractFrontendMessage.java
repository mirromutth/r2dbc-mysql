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

import io.github.mirromutth.r2dbc.mysql.constant.Envelopes;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.backend.DecodeContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;

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
     * @return single {@link ByteBuf}, can not be null, maybe read only.
     */
    protected abstract ByteBuf encodeSingle(ByteBufAllocator bufAllocator, MySqlSession session);

    @Override
    public final Flux<ByteBuf> encode(ByteBufAllocator bufAllocator, AtomicInteger sequenceId, MySqlSession session) {
        requireNonNull(bufAllocator, "bufAllocator must not be null");
        requireNonNull(sequenceId, "sequenceId must not be null");

        return Flux.create(sink -> {
            try {
                ByteBuf allBodyBuf = encodeSingle(bufAllocator, session); // maybe read only buffer

                while (allBodyBuf.readableBytes() >= Envelopes.MAX_PART_SIZE) {
                    ByteBuf headerBuf = bufAllocator.buffer(Envelopes.PART_HEADER_SIZE)
                        .writeMediumLE(Envelopes.MAX_PART_SIZE)
                        .writeByte(sequenceId.getAndIncrement());

                    sink.next(Unpooled.wrappedBuffer(headerBuf, allBodyBuf.readRetainedSlice(Envelopes.MAX_PART_SIZE)));
                }

                // should encode a empty envelope to server even body is empty (like complete frame)

                ByteBuf headerBuf = bufAllocator.buffer(Envelopes.PART_HEADER_SIZE)
                    .writeMediumLE(allBodyBuf.readableBytes())
                    .writeByte(sequenceId.getAndIncrement());

                sink.next(Unpooled.wrappedBuffer(headerBuf, allBodyBuf));
                sink.complete();
            } catch (Throwable e) {
                sink.error(e);
            }
        });
    }

    @Override
    public boolean isExchanged() {
        return true;
    }

    @Override
    public DecodeContext decodeContext() {
        return DecodeContext.normal();
    }
}
