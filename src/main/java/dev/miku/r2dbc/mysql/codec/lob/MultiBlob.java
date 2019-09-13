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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

/**
 * An implementation of {@link ScalarBlob} for multi-{@link Node}s.
 */
final class MultiBlob extends ScalarBlob {

    private final Node[] nodes;

    MultiBlob(Node[] nodes) {
        this.nodes = nodes;
    }

    @Override
    public Flux<ByteBuffer> stream() {
        return Flux.fromArray(this.nodes)
            .map(Node::toByteBuffer)
            .doOnDiscard(Node.class, Node::safeDispose)
            .doOnCancel(this::releaseAll);
    }

    @Override
    public Mono<Void> discard() {
        return Mono.fromRunnable(this::releaseAll);
    }

    private void releaseAll() {
        for (Node node : nodes) {
            node.safeDispose();
        }
    }
}
