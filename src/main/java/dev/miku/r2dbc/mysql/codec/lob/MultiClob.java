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

import dev.miku.r2dbc.mysql.util.ServerVersion;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * An implementation of {@link ScalarClob} for multi-{@link Node}s.
 */
final class MultiClob extends ScalarClob {

    private final Node[] nodes;

    private final int collationId;

    private final ServerVersion version;

    MultiClob(Node[] nodes, int collationId, ServerVersion version) {
        this.nodes = nodes;
        this.collationId = collationId;
        this.version = version;
    }

    @Override
    public Flux<CharSequence> stream() {
        return Flux.fromArray(this.nodes)
            .map(node -> node.toCharSequence(collationId, version))
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
