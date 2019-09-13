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

import dev.miku.r2dbc.mysql.ServerVersion;
import reactor.core.publisher.Mono;

/**
 * An implementation of {@link ScalarClob} for singleton {@link Node}.
 */
final class SingletonClob extends ScalarClob {

    private final Node node;

    private final int collationId;

    private final ServerVersion version;

    SingletonClob(Node node, int collationId, ServerVersion version) {
        this.node = node;
        this.collationId = collationId;
        this.version = version;
    }

    @Override
    public Mono<CharSequence> stream() {
        return Mono.fromSupplier(() -> node.toCharSequence(collationId, version));
    }

    @Override
    public Mono<Void> discard() {
        // No need safety because it is not multi-buffers.
        return Mono.fromRunnable(node::dispose);
    }
}
