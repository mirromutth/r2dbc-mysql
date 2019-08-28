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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RingBuffer}.
 */
class RingBufferTest {

    @Test
    void consume() throws InterruptedException {
        ConcurrentMap<Integer, Boolean> dataMap = new ConcurrentHashMap<>();
        AtomicBoolean succeed = new AtomicBoolean(true);
        BiFunction<Integer, Boolean, Boolean> computer = (key, value) -> {
            if (value == null) {
                return true;
            } else {
                // Exists, impossible.
                succeed.set(false);
                return true;
            }
        };
        Consumer<Integer> consumer = v -> dataMap.compute(v, computer);
        RingBuffer<Integer> buffer = new RingBuffer<>(consumer);
        Lock lock = new ReentrantLock();
        CountDownLatch latch = new CountDownLatch(2);
        Thread thread1 = new Thread(range(buffer, latch, lock, 0, 100, consumer));
        Thread thread2 = new Thread(range(buffer, latch, lock, 100, 200, consumer));
        thread1.start();
        thread2.start();
        latch.await();
        lock.lock();
        try {
            buffer.consume();
        } finally {
            lock.unlock();
        }
        assertTrue(succeed.get());
        Set<Integer> set = new HashSet<>();
        for (int i = 0; i < 200; ++i) {
            set.add(i);
        }
        assertEquals(dataMap.keySet(), set);
    }

    @Test
    void keeping() {
        RingBuffer<Integer> buffer = new RingBuffer<>(v -> {});
        assertEquals(buffer.keeping(1), 1L);
        assertEquals(buffer.keeping(-1), -1L);

        RingBuffer.Event event = new RingBuffer.Event();
        assertEquals(event.keeping(1), 1L);
        assertEquals(event.keeping(-1), -1L);
    }

    private static Runnable range(RingBuffer<Integer> buffer, CountDownLatch latch, Lock lock, int start, int end, Consumer<Integer> consumer) {
        return () -> {
            try {
                for (int data = start; data < end; ++data) {
                    int flag = RingBuffer.FAIL;

                    for (int i = 0; flag == RingBuffer.FAIL && i < 100; ++i) {
                        flag = buffer.tryPush(data);
                    }

                    switch (flag) {
                        case RingBuffer.RICH:
                            break;
                        case RingBuffer.HALF:
                            if (lock.tryLock()) {
                                try {
                                    buffer.consume();
                                } finally {
                                    lock.unlock();
                                }
                            }
                            break;
                        default:
                            lock.lock();
                            try {
                                buffer.consume();
                                consumer.accept(data);
                            } finally {
                                lock.unlock();
                            }
                            break;
                    }
                }
            } finally {
                latch.countDown();
            }
        };
    }
}
