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

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.message.client.ClientMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RequestQueue}.
 */
class RequestQueueTest {

    @Test
    void submit() {
        RequestQueue queue = new RequestQueue();
        List<Integer> arr = new AddEventList(queue);

        Mono<Boolean> third = Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sink, Mono.fromSupplier(() -> arr.add(3)))))
            .flatMap(Function.identity());
        Mono<Boolean> second = Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sink, Mono.fromSupplier(() -> arr.add(2)))))
            .flatMap(Function.identity());
        Mono<Boolean> first = Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sink, Mono.fromSupplier(() -> arr.add(1)))))
            .flatMap(Function.identity());

        Flux.concat(first, second, third)
            .as(StepVerifier::create)
            .expectNext(true, true, true)
            .verifyComplete();

        assertThat(arr).isEqualTo(Arrays.asList(1, 2, 3));
    }

    @Test
    void dispose() {
        RequestQueue queue = new RequestQueue();
        List<Integer> arr = new AddEventList(queue);

        Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sink, Mono.fromSupplier(() -> arr.add(5)))))
            .flatMap(Function.identity())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
        Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sink, Mono.fromSupplier(() -> arr.add(4)))))
            .flatMap(Function.identity())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
        queue.dispose();
        Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sink, Mono.fromSupplier(() -> arr.add(3)))))
            .flatMap(Function.identity())
            .as(StepVerifier::create)
            .verifyError(IllegalStateException.class);
        Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sink, Mono.fromSupplier(() -> arr.add(2)))))
            .flatMap(Function.identity())
            .as(StepVerifier::create)
            .verifyError(IllegalStateException.class);

        assertThat(arr).isEqualTo(Arrays.asList(5, 4));
    }

    @Test
    void disposeWithRelease() {
        RequestQueue queue = new RequestQueue();
        IntegerData[] sources = new IntegerData[] { new IntegerData(1), new IntegerData(2),
            new IntegerData(3), new IntegerData(4) };
        List<Integer> arr = new AddEventList(queue);

        Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sources[0], sink, Mono.fromSupplier(() ->
                arr.add(sources[0].consumeData())))))
            .flatMap(Function.identity())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
        Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sources[1], sink, Mono.fromSupplier(() ->
                arr.add(sources[1].consumeData())))))
            .flatMap(Function.identity())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
        queue.dispose();
        Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sources[2], sink, Mono.fromSupplier(() -> true))))
            .flatMap(Function.identity())
            .as(StepVerifier::create)
            .verifyError(IllegalStateException.class);
        Mono.<Mono<Boolean>>create(sink ->
            queue.submit(RequestTask.wrap(sources[3], sink, Mono.fromSupplier(() -> true))))
            .flatMap(Function.identity())
            .as(StepVerifier::create)
            .verifyError(IllegalStateException.class);

        assertThat(arr).isEqualTo(Arrays.asList(1, 2));
        assertThat(sources).extracting(Disposable::isDisposed).containsOnly(true);
    }

    @Test
    void keeping() {
        RequestQueue queue = new RequestQueue();
        assertThat(queue.keeping(1)).isEqualTo(1L);
        assertThat(queue.keeping(-1)).isEqualTo(-1L);
    }

    private static final class IntegerData extends AtomicInteger implements ClientMessage, Disposable {

        private final int data;

        IntegerData(int data) {
            super(1);
            this.data = data;
        }

        @Override
        public Mono<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
            return Mono.error(IllegalStateException::new);
        }

        int consumeData() {
            dispose();
            return data;
        }

        @Override
        public void dispose() {
            int last = getAndDecrement();
            if (last <= 0) {
                throw new IllegalReferenceCountException(last);
            }
        }

        @Override
        public boolean isDisposed() {
            return get() <= 0;
        }
    }

    private static final class AddEventList extends ArrayList<Integer> {

        private final RequestQueue queue;

        private AddEventList(RequestQueue queue) {
            this.queue = queue;
        }

        @Override
        public boolean add(Integer o) {
            boolean result = super.add(o);
            // Mock request completed.
            queue.run();
            return result;
        }
    }
}
