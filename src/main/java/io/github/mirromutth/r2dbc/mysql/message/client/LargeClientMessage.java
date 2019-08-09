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

import io.github.mirromutth.r2dbc.mysql.internal.ConnectionContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * Base class considers large or unable to calculate length messages for {@link ClientMessage} implementations.
 */
abstract class LargeClientMessage implements ClientMessage {

    /**
     * Returning any length fragments of encoded message that do not care about envelopes.
     *
     * @param allocator the {@link ByteBufAllocator} to use to get a {@link ByteBuf} data buffer to write into.
     * @param context   current MySQL connection context
     * @return encoded fragments of any length
     */
    abstract protected Publisher<ByteBuf> fragments(ByteBufAllocator allocator, ConnectionContext context);

    /**
     * @param allocator the {@link ByteBufAllocator} to use to get a {@link ByteBuf} data buffer to write into.
     * @param context   current MySQL connection context
     * @return lazy loading {@link ByteBuf}s sliced by {@code Envelopes.MAX_ENVELOPE_SIZE}
     */
    @Override
    public Flux<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(context, "context must not be null");

        return Flux.create(sink -> fragments(allocator, context)
            .subscribe(new LargeMessageSlicer(allocator, sink)));
    }
}
