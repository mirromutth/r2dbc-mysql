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

package dev.miku.r2dbc.mysql.codec.lob;

import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class considers single {@link Node} and drains/disposes the {@link Node} on cancellation.
 */
abstract class SingletonLob<T> {

    private final AtomicReference<Node> node;

    SingletonLob(Node node) {
        this.node = new AtomicReference<>(node);
    }

    public final Mono<T> stream() {
        return Mono.defer(() -> {
            Node node = this.node.getAndSet(null);

            if (node == null) {
                return Mono.error(new IllegalStateException("Source has been released"));
            }

            return Mono.just(consume(node));
        });
    }

    public final Mono<Void> discard() {
        return Mono.fromRunnable(() -> {
            Node node = this.node.getAndSet(null);

            if (node != null) {
                node.release();
            }
        });
    }

    protected abstract T consume(Node node);
}
