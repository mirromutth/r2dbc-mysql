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

package dev.miku.r2dbc.mysql.util;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FluxDiscardOnCancel}.
 */
class FluxDiscardOnCancelTest {

    private static final int ROWS = 5;

    @Test
    void allRelease() {
        List<MockRow> rows = createRows();

        Flux.fromIterable(rows)
            .<MockRow>handle((it, sink) -> sink.next(it))
            .as(OperatorUtils::discardOnCancel)
            .doOnDiscard(ReferenceCounted.class, ReferenceCounted::release)
            .<Integer>handle((it, sink) -> {
                try {
                    sink.next(it.id);
                } finally {
                    it.release();
                }
            })
            .take(2)
            .as(StepVerifier::create)
            .expectNext(0, 1)
            .verifyComplete();

        assertThat(rows).hasSize(ROWS).extracting(MockRow::refCnt).containsOnly(0);
    }

    @Test
    void allItems() {
        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .as(OperatorUtils::discardOnCancel)
            .as(StepVerifier::create)
            .expectNext(0, 1, 2, 3)
            .verifyComplete();
    }

    @Test
    void assemblyHook() {
        List<Object> publishers = new ArrayList<>();
        Hooks.onEachOperator(objectPublisher -> {
            publishers.add(objectPublisher);

            return objectPublisher;
        });

        Iterator<Integer> items = createItems(5);

        Flux.fromIterable(() -> items)
            .transform(OperatorUtils::discardOnCancel)
            .as(StepVerifier::create)
            .expectNextCount(5)
            .verifyComplete();

        assertThat(publishers).hasSize(2).anyMatch(it -> it.getClass() == FluxDiscardOnCancel.class);
    }

    @Test
    void considersOnDrop() {
        List<Object> discard = new ArrayList<>();
        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .as(OperatorUtils::discardOnCancel)
            .doOnDiscard(Object.class, discard::add)
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(2)
            .expectNext(0, 1)
            .thenCancel()
            .verify();

        assertThat(discard).containsOnly(2, 3);
    }

    @Test
    void notConsume() {
        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(2)
            .expectNext(0, 1)
            .thenCancel()
            .verify();

        assertThat(items).toIterable().containsSequence(2, 3);
    }

    @Test
    void consumeAndDiscard() {
        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .as(OperatorUtils::discardOnCancel)
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(2)
            .expectNext(0, 1)
            .thenCancel()
            .verify();

        assertThat(items).toIterable().isEmpty();
    }

    static Iterator<Integer> createItems(int count) {
        return IntStream.range(0, count).boxed().iterator();
    }

    static List<MockRow> createRows() {
        return IntStream.range(0, ROWS).mapToObj(MockRow::new).collect(Collectors.toList());
    }

    private static final class MockRow extends AbstractReferenceCounted {

        private int id;

        MockRow(int id) {
            this.id = id;
        }

        @Override
        public ReferenceCounted touch(Object o) {
            return this;
        }

        @Override
        protected void deallocate() {
            this.id = -1;
        }
    }
}
