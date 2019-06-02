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
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Flux;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Every client message need slice huge package to multi-envelopes.
 */
abstract class AbstractClientMessage implements ClientMessage {

    /**
     * Encode to single {@link ByteBuf} without packet header
     *
     * @param allocator the {@link ByteBufAllocator} to use to get a {@link ByteBuf} data buffer to write into.
     * @param session   current MySQL session.
     * @return single {@link ByteBuf}, can not be null, maybe read only.
     */
    protected abstract ByteBuf encodeSingle(ByteBufAllocator allocator, MySqlSession session);

    @Override
    public Flux<ByteBuf> encode(ByteBufAllocator allocator, MySqlSession session) {
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(session, "session must not be null");

        return Flux.create(sink -> {
            ByteBuf allBodyBuf = encodeSingle(allocator, session);

            while (allBodyBuf.readableBytes() >= Envelopes.MAX_PART_SIZE) {
                sink.next(allBodyBuf.readRetainedSlice(Envelopes.MAX_PART_SIZE));
            }

            // Should always write the envelope to server even body is empty. (like complete frame)
            sink.next(allBodyBuf);
            sink.complete();
        });
    }
}
