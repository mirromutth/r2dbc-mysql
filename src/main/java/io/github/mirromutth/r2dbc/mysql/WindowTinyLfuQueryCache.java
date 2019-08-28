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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.require;

/**
 * An implementation of {@link QueryCache} which based on Window Tiny LFU.
 * <p>
 * The page replacement algorithm has kept eventual consistency with the hashing map like Caffeine,
 * but using a different and cleanly implementation based on special features, that are unable to remove
 * any element by user actively, and only load the element when it does not exist in the cache.
 * <p>
 * In other words, this W-TinyLFU implementation has only {@literal computeIfAbsent} and highly optimized
 * by this feature. So it will never support actively getIfPresent/putOverlay/removeIfPresent until
 * refactor to other implementation.
 * <p>
 * It is thread-safety.
 *
 * @see #consume(Lru.Node) only remove element in this method.
 */
final class WindowTinyLfuQueryCache implements QueryCache {

    private static final int CAS_TIMES = 100;

    private static final Function<String, Lru.Node<Query>> COMPUTER = sql -> new Lru.Node<>(sql, Query.parse(sql));

    private final ConcurrentMap<String, Lru.Node<Query>> hashing = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();

    private final Lru<Query> window;

    private final Slru<Query> slru;

    private final FrequencySketch sketch;

    private final RingBuffer<Lru.Node<Query>> buffer;

    WindowTinyLfuQueryCache(int capacity) {
        require(capacity > 0, "capacity must be a positive integer");

        int windowSize = capacity / 100;

        this.window = new Lru<>(Math.max(1, windowSize));
        this.slru = new Slru<>(Math.max(1, capacity - windowSize));
        this.sketch = new FrequencySketch(capacity);
        this.buffer = new RingBuffer<>(this::consume);
    }

    @Override
    public Query getOrParse(String sql) {
        Lru.Node<Query> node = hashing.computeIfAbsent(sql, COMPUTER);
        push(node);
        return node.getValue();
    }

    private void push(Lru.Node<Query> node) {
        int flag = RingBuffer.FAIL;

        // Try until CAS no fail or exceed the limit of times.
        for (int i = 0; flag == RingBuffer.FAIL && i < CAS_TIMES; ++i) {
            flag = buffer.tryPush(node);
        }

        switch (flag) {
            case RingBuffer.RICH: // Succeed
                // Buffer is rich, no need do anything.
                break;
            case RingBuffer.HALF: // Succeed, but filled half, try do something.
                if (lock.tryLock()) {
                    try {
                        buffer.consume();
                    } finally {
                        lock.unlock();
                    }
                }
                break;
            // case RingBuffer.FULL: // Failed because full.
            // case RingBuffer.FAIL: // Failed because CAS failed.
            default:
                // Must do something before too late, blocking current thread.
                lock.lock();
                try {
                    buffer.consume();
                    consume(node);
                } finally {
                    lock.unlock();
                }
                break;
        }
    }

    private void consume(Lru.Node<Query> node) {
        String sql = node.getKey();
        Lru<Query> lru = node.getLru();

        if (lru == null) {
            // If lru is null, node may be not newest node, maybe it has removed by previous elements.
            // And then, if access same key at this moment, hashing will compute a new node.
            // Make sure current node is newest node, find or compute from hashing.
            // If lru is not null, because node should be re-init before remove from LRU (remove from hashing
            // should remove from LRU first), so it never been removed, and node must be newest, no need do
            // more things.
            node = hashing.computeIfAbsent(sql, COMPUTER);
            lru = node.getLru();
            // Hashing will not be delete again before we complete process of all consumes.
        }

        sketch.increment(sql.hashCode());

        if (lru == null) {
            // No cache contains it, it is a new member.
            Lru.Node<Query> windowEvicted = window.push(node);

            if (windowEvicted == null) {
                return;
            }

            Lru.Node<Query> slruEvicted = slru.nextEviction();

            if (slruEvicted == null) {
                // SLRU will be not evict any node, just add it to SLRU, no-one is victim.
                slru.push(windowEvicted);
                return;
            }

            // Need to check if it is joining SLRU worthily or not.
            // Let's choose victim between windowEvicted and slruEvicted! Ready for the Battle!
            int windowHashCode = windowEvicted.getKey().hashCode();
            int slruHashCode = slruEvicted.getKey().hashCode();

            Lru.Node<Query> victim;

            if (sketch.frequency(windowHashCode) > sketch.frequency(slruHashCode)) {
                // Victim is also slruEvicted.
                victim = slru.push(windowEvicted);
            } else {
                // Window has evicted from LRU list, just remove.
                victim = windowEvicted;
            }

            if (victim == null) {
                // Impossible path, because victim should not be null.
                return;
            }

            hashing.remove(victim.getKey(), victim);
        } else if (lru == window) {
            window.refresh(node);
        } else {
            slru.refresh(node);
        }
    }
}
