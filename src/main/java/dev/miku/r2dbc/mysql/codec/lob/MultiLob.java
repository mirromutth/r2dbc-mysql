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

import dev.miku.r2dbc.mysql.util.OperatorUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Base class considers multiple {@link Node}s and drains/disposes {@link Node}s on cancellation.
 */
abstract class MultiLob {

    private static final Consumer<Node> RELEASE = Node::release;

    private final AtomicReference<Node[]> nodes;

    MultiLob(Node[] nodes) {
        this.nodes = new AtomicReference<>(nodes);
    }

    final Flux<Node> nodes() {
        return Flux.defer(() -> {
            Node[] nodes = this.nodes.getAndSet(null);

            if (nodes == null) {
                return Flux.error(new IllegalStateException("Source has been released"));
            }

            return OperatorUtils.discardOnCancel(Flux.fromArray(nodes))
                .doOnDiscard(Node.class, RELEASE);
        });
    }

    public final Mono<Void> discard() {
        return Mono.fromRunnable(() -> {
            Node[] nodes = this.nodes.getAndSet(null);

            if (nodes != null) {
                Node.releaseAll(nodes);
            }
        });
    }
}
