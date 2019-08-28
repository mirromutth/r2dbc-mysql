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

package io.github.mirromutth.r2dbc.mysql;

import io.github.mirromutth.r2dbc.mysql.client.Client;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.require;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link StatementCache} which based on Window Tiny LFU.
 * <p>
 * It is just a minimal implementation of Window Tiny LFU not like Caffeine (inspired by the paper arXiv:1512.00727 [cs.OS]),
 * does not have features such as dynamic window, time-based expires, etc.
 * <p>
 * It is NOT thread-safety.
 */
final class WindowTinyLfuStatementCache implements StatementCache {

    // Maybe use BiMap?
    private final Map<String, Lru.Node<Mono<Integer>>> hashing = new HashMap<>();

    private final Client client;

    private final Lru<Mono<Integer>> window;

    private final Slru<Mono<Integer>> slru;

    private final FrequencySketch sketch;

    WindowTinyLfuStatementCache(Client client, int capacity) {
        require(capacity > 0, "capacity must be a positive integer");

        int windowSize = capacity / 100;

        this.client = client;
        this.window = new Lru<>(Math.max(1, windowSize));
        this.slru = new Slru<>(Math.max(1, capacity - windowSize));
        this.sketch = new FrequencySketch(capacity);
    }

    @Override
    public Mono<Integer> getOrPrepare(String sql) {
        requireNonNull(sql, "sql must not be null");

        return Mono.defer(() -> {
            sketch.increment(sql.hashCode());

            Lru.Node<Mono<Integer>> node = hashing.get(sql);

            if (node != null) {
                if (node.getLru() == window) {
                    window.refresh(node);
                } else {
                    slru.refresh(node);
                }
                return node.getValue();
            }

            Mono<Integer> result = QueryFlow.prepare(client, sql).cache();
            node = new Lru.Node<>(sql, result);
            hashing.put(sql, node);

            Lru.Node<Mono<Integer>> windowEvicted = window.push(node);

            if (windowEvicted == null) {
                return result;
            }

            Lru.Node<Mono<Integer>> slruEvicted = slru.nextEviction();

            if (slruEvicted == null) {
                // SLRU will be not evict any node, just add it to SLRU, no-one is victim.
                slru.push(windowEvicted);
                return result;
            }

            // Need to check if it is joining SLRU worthily or not.
            // Let's choose victim between windowEvicted and slruEvicted! Ready for the Battle!
            int windowHashCode = windowEvicted.getKey().hashCode();
            int slruHashCode = slruEvicted.getKey().hashCode();

            Lru.Node<Mono<Integer>> victim;

            if (sketch.frequency(windowHashCode) > sketch.frequency(slruHashCode)) {
                // Victim is also slruEvicted.
                victim = slru.push(windowEvicted);
            } else {
                // Window has evicted from LRU list, just close and remove.
                victim = windowEvicted;
            }

            if (victim == null) {
                // Impossible path, because victim should not be null.
                return result;
            }

            String victimSql = victim.getKey();

            return victim.getValue().flatMap(id -> QueryFlow.close(client, id))
                .doOnTerminate(() -> hashing.remove(victimSql))
                .then(result);
        });
    }
}
