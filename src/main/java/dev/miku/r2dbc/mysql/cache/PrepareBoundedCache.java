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

package dev.miku.r2dbc.mysql.cache;

import java.util.HashMap;
import java.util.function.IntConsumer;

/**
 * A bounded implementation of {@link PrepareCache} that uses synchronized methods to ensure correctness, even
 * it should not be used thread concurrently.
 */
final class PrepareBoundedCache extends HashMap<String, Lru.Node<Integer>> implements PrepareCache {

    private final FreqSketch sketch;

    private final Lru<Integer> window;

    private final Lru<Integer> probation;

    private final Lru<Integer> protection;

    PrepareBoundedCache(int capacity) {
        int windowSize = Math.max(1, capacity / 100);
        int protectionSize = Math.max(1, (int) ((capacity - windowSize) * 0.8));
        int probationSize = Math.max(1, capacity - protectionSize - windowSize);

        this.sketch = new FreqSketch(windowSize + protectionSize + probationSize);
        this.window = new Lru<>(windowSize, Lru.WINDOW);
        this.probation = new Lru<>(probationSize, Lru.PROBATION);
        this.protection = new Lru<>(protectionSize, Lru.PROTECTION);
    }

    @Override
    public synchronized Integer getIfPresent(String key) {
        Lru.Node<Integer> node = super.get(key);

        if (node == null) {
            return null;
        }

        drainRead(node);
        return node.getValue();
    }

    @Override
    public synchronized boolean putIfAbsent(String key, int value, IntConsumer evict) {
        Lru.Node<Integer> wantAdd = new Lru.Node<>(key, value);
        Lru.Node<Integer> present = super.putIfAbsent(key, wantAdd);

        if (present == null) {
            drainAdded(wantAdd, evict);
            return true;
        }

        drainRead(present);
        return false;
    }

    @Override
    public String toString() {
        return window.toString() + probation + protection;
    }

    private void drainRead(Lru.Node<Integer> node) {
        sketch.increment(node.getKey().hashCode());

        switch (node.getLru()) {
            case Lru.WINDOW:
                window.refresh(node);
                break;
            case Lru.PROBATION:
                probation.remove(node);
                Lru.Node<Integer> evicted = protection.push(node);

                if (evicted != null) {
                    // This element must be protected.
                    // Result should be null because probation has removed one element.
                    probation.push(evicted);
                }
                break;
            case Lru.PROTECTION:
                protection.refresh(node);
                break;
            default:
                throw new IllegalStateException("The element of cache is not contained in any segment");
        }
    }

    private void drainAdded(Lru.Node<Integer> node, IntConsumer evict) {
        sketch.increment(node.getKey().hashCode());

        Lru.Node<Integer> windowEvict = window.push(node);
        if (windowEvict == null) {
            return;
        }

        Lru.Node<Integer> probationEvict = probation.nextEviction();
        if (probationEvict == null) {
            // Probation will be not evict any node, no-one is evicted.
            probation.push(windowEvict);
            return;
        }

        Lru.Node<Integer> evicted = sketch.frequency(windowEvict.getKey().hashCode()) >
            sketch.frequency(probationEvict.getKey().hashCode()) ?
            probation.push(windowEvict) : windowEvict;

        if (evicted == null) {
            // Impossible path, because probation eviction or window eviction should not be null.
            return;
        }

        super.remove(evicted.getKey(), evicted);
        evict.accept(evicted.getValue());
    }
}
