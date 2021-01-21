/*
 * Copyright 2018-2020 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class considers single {@link ByteBuf} and drains/disposes the {@link ByteBuf} on cancellation.
 *
 * @param <T> the emit data type, it should be {@code ByteBuffer} or {@link CharSequence}.
 */
abstract class SingletonLob<T> {

    private final AtomicReference<ByteBuf> buf;

    SingletonLob(ByteBuf buf) {
        this.buf = new AtomicReference<>(buf);
    }

    public final Mono<T> stream() {
        return Mono.defer(() -> {
            ByteBuf buf = this.buf.getAndSet(null);

            if (buf == null) {
                return Mono.error(new IllegalStateException("Source has been released"));
            }

            try {
                return Mono.just(convert(buf));
            } finally {
                buf.release();
            }
        });
    }

    public final Mono<Void> discard() {
        return Mono.fromRunnable(() -> {
            ByteBuf buf = this.buf.getAndSet(null);

            if (buf != null) {
                buf.release();
            }
        });
    }

    protected abstract T convert(ByteBuf buf);
}
