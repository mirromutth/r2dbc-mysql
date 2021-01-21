/*
 * Copyright 2018-2021 the original author or authors.
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

package dev.miku.r2dbc.mysql.codec.lob;

import dev.miku.r2dbc.mysql.util.NettyBufferUtils;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Base class considers multiple {@link ByteBuf}s and drains/disposes {@link ByteBuf}s on cancellation.
 *
 * @param <T> the emit data type, it should be {@code ByteBuffer} or {@link CharSequence}.
 */
abstract class MultiLob<T> {

    private static final Consumer<ByteBuf> RELEASE = ByteBuf::release;

    private final AtomicReference<List<ByteBuf>> buffers;

    MultiLob(List<ByteBuf> buffers) {
        this.buffers = new AtomicReference<>(buffers);
    }

    public final Flux<T> stream() {
        return Flux.defer(() -> {
            List<ByteBuf> buffers = this.buffers.getAndSet(null);

            if (buffers == null) {
                return Flux.error(new IllegalStateException("Source has been released"));
            }

            return OperatorUtils.discardOnCancel(Flux.fromIterable(buffers))
                .doOnDiscard(ByteBuf.class, RELEASE)
                .map(this::consume);
        });
    }

    public final Mono<Void> discard() {
        return Mono.fromRunnable(() -> {
            List<ByteBuf> buffers = this.buffers.getAndSet(null);

            if (buffers != null) {
                NettyBufferUtils.releaseAll(buffers);
            }
        });
    }

    protected abstract T convert(ByteBuf buf);

    private T consume(ByteBuf buf) {
        try {
            return convert(buf);
        } finally {
            buf.release();
        }
    }
}
