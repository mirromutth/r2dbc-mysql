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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.client.Client;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * An identifier considers about a prepared statement should close only once, and closed status.
 */
final class PreparedIdentifier extends AtomicBoolean {

    private final int id;

    private final Supplier<Mono<Void>> closer;

    PreparedIdentifier(Client client, int id) {
        this.id = id;
        this.closer = () -> {
            if (compareAndSet(false, true)) {
                return QueryFlow.close(client, id);
            }

            return Mono.empty();
        };
    }

    int getId() {
        return id;
    }

    boolean isClosed() {
        return get();
    }

    Mono<Void> close() {
        return Mono.defer(closer);
    }
}
