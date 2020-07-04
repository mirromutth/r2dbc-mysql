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

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.message.client.ClientMessage;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

/**
 * A task for execute, propagate errors and release resources.
 * <p>
 * If task executed, resources should been released by {@code supplier} instead of task self.
 */
final class RequestTask<T> {

    @Nullable
    private final Disposable disposable;

    private final MonoSink<T> sink;

    private final T supplier;

    private RequestTask(@Nullable Disposable disposable, MonoSink<T> sink, T supplier) {
        this.disposable = disposable;
        this.sink = sink;
        this.supplier = supplier;
    }

    void run() {
        sink.success(supplier);
    }

    /**
     * Cancel task and release resources.
     *
     * @param e cancelled by which error
     */
    void cancel(Throwable e) {
        if (disposable != null) {
            disposable.dispose();
        }
        sink.error(e);
    }

    static <T> RequestTask<T> wrap(ClientMessage message, MonoSink<T> sink, T supplier) {
        if (message instanceof Disposable) {
            return new RequestTask<>((Disposable) message, sink, supplier);
        }

        return new RequestTask<>(null, sink, supplier);
    }

    static <T> RequestTask<T> wrap(Flux<? extends ClientMessage> messages, MonoSink<T> sink, T supplier) {
        return new RequestTask<>(new DisposableFlux(messages), sink, supplier);
    }

    static <T> RequestTask<T> wrap(MonoSink<T> sink, T supplier) {
        return new RequestTask<>(null, sink, supplier);
    }

    private static final class DisposableFlux implements Disposable {

        private final Flux<? extends ClientMessage> messages;

        private DisposableFlux(Flux<? extends ClientMessage> messages) {
            this.messages = messages;
        }

        @Override
        public void dispose() {
            Flux.from(messages).subscribe(it -> {
                if (it instanceof Disposable) {
                    ((Disposable) it).dispose();
                }
            });
        }
    }
}
