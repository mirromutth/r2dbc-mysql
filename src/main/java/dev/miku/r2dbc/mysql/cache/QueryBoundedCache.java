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

package dev.miku.r2dbc.mysql.cache;

import dev.miku.r2dbc.mysql.Query;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded implementation of {@link QueryCache} that supports high expected concurrency.
 */
final class QueryBoundedCache extends ConcurrentHashMap<String, Lru.Node<Query>> implements QueryCache {

    private static final int READ_BUFFER_SIZE = 16;

    private static final int WRITE_BUFFER_SIZE = 32;

    private final FreqSketch sketch;

    private final RingBuffer<Lru.Node<Query>> readBuffer;

    private final RingBuffer<Lru.Node<Query>> writeBuffer;

    private final ReentrantLock lock;

    private final Lru<Query> window;

    private final Lru<Query> probation;

    private final Lru<Query> protection;

    QueryBoundedCache(int capacity) {
        int windowSize = Math.max(1, capacity / 100);
        int protectionSize = Math.max(1, (int) ((capacity - windowSize) * 0.8));
        int probationSize = Math.max(1, capacity - protectionSize - windowSize);

        this.sketch = new FreqSketch(windowSize + protectionSize + probationSize);
        this.readBuffer = new RingBuffer<>(READ_BUFFER_SIZE, 3, this::drainRead);
        this.writeBuffer = new RingBuffer<>(WRITE_BUFFER_SIZE, 50, this::drainAdded);
        this.lock = new ReentrantLock();
        this.window = new Lru<>(windowSize, Lru.WINDOW);
        this.probation = new Lru<>(probationSize, Lru.PROBATION);
        this.protection = new Lru<>(protectionSize, Lru.PROTECTION);
    }

    @Override
    public Query get(String key) {
        // An optimistic fast path to avoid unnecessary locking.
        Lru.Node<Query> node = super.get(key);
        if (node != null) {
            afterRead(node);
            return node.getValue();
        }

        boolean[] present = new boolean[] { true };

        // It always return current (existing or computed) value.
        node = super.computeIfAbsent(key, (k) -> {
            present[0] = false;
            return new Lru.Node<>(k, Query.parse(k));
        });

        if (present[0]) {
            afterRead(node);
        } else {
            afterAdded(node);
        }

        return node.getValue();
    }

    private void afterRead(Lru.Node<Query> node) {
        boolean isFailed = !readBuffer.offer(node);

        /*
         * Inspired by Caffeine:
         *
         * When a cache hit occurs then processing the node is a policy hint, e.g. to LRU reorder. This is
         * best-effort and the work could be dropped, e.g. if the buffer is full (or CAS fails). This
         * favors concurrent throughput and likely the same popular entries are being accessed. If so, the
         * eviction policy is already going to favor them so being strict won't change the hit rate
         * substantially. Being lossy can improve the system throughput, so generally a favorable tradeoff.
         *
         * So there is using tryLock instead of lock for performance, even offer fails.
         */
        if (lock.tryLock()) {
            try {
                readBuffer.drainAll();
                if (isFailed) {
                    drainRead(node);
                }
                writeBuffer.drainAll();
            } finally {
                lock.unlock();
            }
        }
    }

    private void afterAdded(Lru.Node<Query> node) {
        /*
         * When a cache miss occurs then the entry must be added to the LRU and maybe an eviction occurs.
         * This must occur, so it has the work has to be processed and not dropped.
         *
         * So there is using lock when offer fails.
         */
        if (writeBuffer.offer(node)) {
            if (lock.tryLock()) {
                try {
                    readBuffer.drainAll();
                    writeBuffer.drainAll();
                } finally {
                    lock.unlock();
                }
            }
        } else {
            lock.lock();
            try {
                readBuffer.drainAll();
                writeBuffer.drainAll();
                drainAdded(node);
            } finally {
                lock.unlock();
            }
        }
    }

    private void drainAdded(Lru.Node<Query> node) {
        sketch.increment(node.getKey().hashCode());

        Lru.Node<Query> windowEvict = window.push(node);
        if (windowEvict == null) {
            return;
        }

        Lru.Node<Query> probationEvict = probation.nextEviction();
        if (probationEvict == null) {
            // Probation will be not evict any node, no-one is evicted.
            probation.push(windowEvict);
            return;
        }

        Lru.Node<Query> evicted = sketch.frequency(windowEvict.getKey().hashCode()) >
            sketch.frequency(probationEvict.getKey().hashCode()) ?
            probation.push(windowEvict) : windowEvict;

        if (evicted == null) {
            // Impossible path, because probation eviction or window eviction should not be null.
            return;
        }

        super.remove(evicted.getKey(), evicted);
    }

    private void drainRead(Lru.Node<Query> node) {
        sketch.increment(node.getKey().hashCode());

        switch (node.getLru()) {
            case Lru.WINDOW:
                window.refresh(node);
                break;
            case Lru.PROBATION:
                probation.remove(node);
                Lru.Node<Query> evicted = protection.push(node);

                if (evicted != null) {
                    // This element must be protected.
                    // Result should be null because probation has removed one element.
                    probation.push(evicted);
                }
                break;
            case Lru.PROTECTION:
                protection.refresh(node);
                break;
            // default: break; // It was evicted by last wrote, or was holden in write buffer.
        }
    }
}
