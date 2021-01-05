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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * Avoid false sharding. Assume the cache line is 128 bytes.
 */
abstract class LeftPad {

    long p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14;
}

/**
 * A non-blocking bounded ring buffer.
 *
 * @param <T> the type of elements in buffer
 */
final class RingBuffer<T> extends LeftPad {

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<RingBuffer> HEAD =
        AtomicIntegerFieldUpdater.newUpdater(RingBuffer.class, "head");

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<RingBuffer> TAIL =
        AtomicIntegerFieldUpdater.newUpdater(RingBuffer.class, "tail");

    /**
     * The index scale of {@link Object} arrays. Don't use {@code Unsafe} because it may be unstable.
     */
    private static final int SCALE = 4;

    private static final int SHIFT = Integer.numberOfTrailingZeros(128) -
        Integer.numberOfTrailingZeros(SCALE);

    private static final int PADDING = (1 << SHIFT) - 1;

    private static final int MAX_SIZE = 256; // 256 * 128 + 127 = 32768 + 127, should not too large

    private volatile int head; // p15 1st part

    int p15; // p15 2nd part

    long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;

    private volatile int tail; // p31 1st part

    int p31; // p31 2nd part

    long p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46;

    private final AtomicReferenceArray<T> buffer;

    private final int maxSize;

    private final int mask;

    private final int retry;

    private final Consumer<T> consumer;

    RingBuffer(int capacity, int retry, Consumer<T> consumer) {
        require(capacity > 0 && capacity <= MAX_SIZE, "capacity must between 1 and 256");

        int maxSize = Caches.ceilingPowerOfTwo(capacity);

        this.maxSize = maxSize;
        this.mask = maxSize - 1;
        this.buffer = new AtomicReferenceArray<>((maxSize << SHIFT) + PADDING);
        this.retry = retry;
        this.consumer = consumer;
    }

    /**
     * Should NEVER use &lt; or &gt; to compare {@link #head} and {@link #tail} directly, because they may
     * overflow. Calculate {@code tail - head} to check current buffer size.
     *
     * @param value the element to be offered.
     * @return {@code true} if succeed, otherwise failed.
     */
    boolean offer(T value) {
        for (int i = 0; i < retry; ++i) {
            int tail = this.tail;
            int size = tail - this.head;

            if (size >= this.maxSize) {
                return false;
            }

            /*
             * Increment first, set value second. Pre-calculate offset for minimize
             * the process between tail increment and value set. Maybe use `lazySet`
             * for value set? Currently, consider using set to be immediately visible
             * to other threads.
             */
            int offset = getOffset(tail);
            if (TAIL.compareAndSet(this, tail, tail + 1)) {
                this.buffer.set(offset, value);
                return true;
            }
        }

        return false;
    }

    /**
     * Should NEVER use &lt; or &gt; to compare {@link #head} and {@link #tail} directly, because they may
     * overflow. Calculate {@code tail - head} to check current buffer size.
     */
    void drainAll() {
        // Consume to current tail, do NOT extract tail in each loop to avoid inf loop.
        // And this can avoid to getting unclear value in loop.
        // Must read head first to avoid race.
        int head = this.head;
        int tail = this.tail;
        int size = tail - head;

        if (size < 0 || size > this.maxSize) {
            throw new IllegalStateException("Impossible size between " + head + " and " + tail);
        } else if (size == 0) {
            return;
        }

        // Counting null value to avoid inf loop.
        int nullCount = 0, maxNullCount = Math.min(size << 2, 128);

        // The size may be negative when other thread consuming and another one offering this buffer.
        for (; size > 0; size = tail - (head = this.head)) {
            int offset = getOffset(head);
            T value = buffer.get(offset);

            if (value == null) {
                // Tail was increased but value hasn't set, or has already been consumed by other thread.
                if (nullCount++ < maxNullCount) {
                    // Try again.
                    continue;
                }

                // An intense race condition, quit.
                break;
            }

            if (HEAD.compareAndSet(this, head, head + 1)) {
                // Element may be offered immediately when the head increment, then new element
                // will be wrote to last location. So using CAS here to avoid rewrite the new
                // element. And we don't care if CAS succeeds or fails.
                buffer.compareAndSet(offset, value, null);
                consumer.accept(value);
            }
        }
    }

    long keeping(int v) {
        return p0 = p1 = p2 = p3 = p4 = p5 = p6 = p8 = p9 = p10 = p11 = p12 = p13 = p14 =
            p16 = p17 = p18 = p19 = p20 = p21 = p22 = p23 = p24 = p25 = p26 = p27 = p28 = p29 = p30 =
                p32 = p33 = p34 = p35 = p36 = p37 = p38 = p39 = p40 = p41 = p42 = p43 = p44 = p45 = p46 =
                    p7 = p15 = p31 = v;
    }

    private int getOffset(int index) {
        return ((index & mask) << SHIFT) + PADDING;
    }
}
