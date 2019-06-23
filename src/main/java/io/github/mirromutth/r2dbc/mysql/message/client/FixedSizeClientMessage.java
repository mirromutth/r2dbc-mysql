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
import reactor.core.publisher.Mono;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ClientMessage} considers the message can be encoded as one envelope.
 */
abstract class FixedSizeClientMessage implements ClientMessage {

    /**
     * @return MUST less than {@link Envelopes#MAX_ENVELOPE_SIZE}
     */
    abstract protected int size();

    abstract protected void writeTo(ByteBuf buf);

    @Override
    public Mono<ByteBuf> encode(ByteBufAllocator allocator, MySqlSession session) {
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(session, "session must not be null");

        return Mono.fromSupplier(() -> {
            int s = size();
            ByteBuf buf = allocator.buffer(s, s);

            try {
                writeTo(buf);
                return buf;
            } catch (Throwable e) {
                buf.release();
                throw e;
            }
        });
    }
}
