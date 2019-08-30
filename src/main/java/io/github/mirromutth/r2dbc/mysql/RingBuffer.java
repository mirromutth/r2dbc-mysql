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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

/**
 * Cache line left padding for False Sharding. This assumes that the cache is 64 bytes.
 */
@SuppressWarnings("WeakerAccess")
abstract class LeftPad {

    protected long p0, p1, p2, p3, p4, p5, p6;
}

/**
 * Cache line padding for read index and write index of {@link RingBuffer}.
 * <p>
 * Note: AtomicLongFieldUpdater maybe provided by lock-based implementation,
 * and this ring buffer no need a unique sequence not like Disruptor,
 * so just use AtomicIntegerFieldUpdater, otherwise we need create a {@code UnsafeUtils}
 * and use the unpublished API of {@code Unsafe} (sun.misc). Of course, use {@code int}
 * means it maybe overflow to a negative integer, should programming be carefully.
 */
@SuppressWarnings("WeakerAccess")
abstract class RingBufferPad extends LeftPad {

    protected static final AtomicIntegerFieldUpdater<RingBufferPad> TAIL_UPDATER = AtomicIntegerFieldUpdater.newUpdater(RingBufferPad.class, "tail");

    protected volatile int head; // p7 1st part

    protected int p7; // p7 2nd part

    protected long p8, p9, p10, p11, p12, p13, p14;

    protected volatile int tail; // p15 1st part

    protected int p15; // p15 2nd part

    protected long p16, p17, p18, p19, p20, p21, p22;
}

/**
 * Cache line padding for {@link RingBuffer} event.
 */
@SuppressWarnings("WeakerAccess")
abstract class EventPad extends LeftPad {

    protected volatile Object value; // p7

    protected long p8, p9, p10, p11, p12, p13, p14;
}

/**
 * A non-blocking bounded ring buffer.
 */
final class RingBuffer<T> extends RingBufferPad {

    private static final int FULL = -1;

    static final int FAIL = 0;

    static final int HALF = 1;

    static final int RICH = 2;

    private static final int SIZE = 32;

    private static final int MASK = SIZE - 1;

    private final Event[] array;

    private final Consumer<T> consumer;

    RingBuffer(Consumer<T> consumer) {
        Event[] array = new Event[SIZE];

        for (int i = 0; i < SIZE; ++i) {
            array[i] = new Event();
        }

        this.array = array;
        this.consumer = consumer;
    }

    int tryPush(T value) {
        int tail = this.tail; // Maybe negative
        int size = tail - this.head; // Correctly size even overflow

        if (size >= SIZE) {
            return FULL;
        }

        if (TAIL_UPDATER.compareAndSet(this, tail, tail + 1)) {
            array[tail & MASK].value = value;

            if (size >= (SIZE >>> 1)) {
                return HALF;
            } else {
                return RICH;
            }
        } else {
            return FAIL;
        }
    }

    /**
     * Must lock before call this method.
     */
    void consume() {
        int tail = this.tail; // Maybe negative
        int head = this.head; // Maybe negative
        int size = tail - head; // Correctly size even overflow

        if (size == 0) {
            return;
        }

        if (size > SIZE) {
            // Impossible path, but should check it.
            throw new IllegalStateException(String.format("Ring buffer head pointer is %d and tail pointer is %d, impossible", head, tail));
        }

        do {
            int index = head & MASK;
            Event e = array[index];

            if (e.value == null) {
                // The value has not initialize, means consume done.
                break;
            }

            @SuppressWarnings("unchecked")
            T value = (T) e.value;
            e.value = null;

            consumer.accept(value);

            ++head;
        } while (head != tail); // Should NEVER use `<` or `>`, because they are maybe overflow.

        this.head = head;
    }

    long keeping(int v) {
        return p0 = p1 = p2 = p3 = p4 = p5 = p6 = p8 = p9 = p10 = p11 = p12 = p13 = p14 = p16 = p17 = p18 = p19 = p20 = p21 = p22 = p7 = p15 = v;
    }

    static final class Event extends EventPad {

        long keeping(int v) {
            return p0 = p1 = p2 = p3 = p4 = p5 = p6 = p8 = p9 = p10 = p11 = p12 = p13 = p14 = v;
        }
    }
}
