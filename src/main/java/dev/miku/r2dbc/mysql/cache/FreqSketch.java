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

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * A probabilistic set for estimating the popularity (frequency) of an element within an access frequency
 * based time window. The maximum frequency of an element is limited to 15 (4-bits) and an aging process
 * periodically halves the popularity of all elements.
 * <p>
 * Notice: it's using lots of magic numbers, and should not to change these numbers.
 */
final class FreqSketch {

    /*
     * This is a SLIGHTLY altered version of Caffeine's [1] implementation.
     *
     * This class maintains a 4-bit CountMinSketch [2] with periodic aging to provide the popularity
     * history for the TinyLfu admission policy [3]. The time and space efficiency of the sketch allows it
     * to cheaply estimate the frequency of an entry in a stream of cache access events.
     *
     * The counter matrix is represented as a single dimensional array holding 16 counters per slot. A
     * fixed depth of four balances the accuracy and cost, resulting in a width of four times the length of
     * the array. To retain an accurate estimation the array's length equals the maximum number of entries
     * in the cache, increased to the closest power-of-two to exploit more efficient bit masking. This
     * configuration results in a confidence of 93.75% and error bound of e / width.
     *
     * The frequency of all entries is aged periodically using a sampling window based on the maximum
     * number of entries in the cache. This is referred to as the reset operation by TinyLfu and keeps the
     * sketch fresh by dividing all counters by two and subtracting based on the number of odd counters
     * found. The O(n) cost of aging is amortized, ideal for hardware pre-fetching, and uses inexpensive
     * bit manipulations per array location.
     *
     * [1] Caffeine: a high performance, near optimal caching library based on Java 8.
     * https://github.com/ben-manes/caffeine
     * [2] An Improved Data Stream Summary: The Count-Min Sketch and its Applications
     * http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
     * [3] TinyLFU: A Highly Efficient Cache Admission Policy
     * https://dl.acm.org/citation.cfm?id=3149371
     */

    /**
     * A mixture of seeds from FNV-1a, CityHash, and Murmur3.
     */
    private static final long[] SEED = { 0xc3a5c85c97cb3127L, 0xb492b66fbe98f273L,
        0x9ae16a3b2f90404fL, 0xcbf29ce484222325L };

    private static final long RESET_MASK = 0x7777777777777777L;

    private static final long ONE_MASK = 0x1111111111111111L;

    private static final int MAX_SIZE = Integer.MIN_VALUE >>> 1;

    private final long[] table;

    private final int tableMask;

    private final int sampling;

    private int size;

    /**
     * Construct {@code FreqSketch} with a fixed size.
     *
     * @param maxSize the fixed size of the sketch, at least 1.
     */
    FreqSketch(int maxSize) {
        require(maxSize > 0, "maxSize must be a positive integer");

        int capacity = Math.min(maxSize, MAX_SIZE);
        int sampling = capacity * 10;
        int length = Caches.ceilingPowerOfTwo(capacity);

        this.table = new long[length];
        this.tableMask = length - 1;
        this.sampling = sampling > 0 ? sampling : Integer.MAX_VALUE;
        this.size = 0;
    }

    /**
     * Returns the estimated number of occurrences of an element, possibly zero but never negative, up to the
     * maximum (15).
     *
     * @param hashCode the hash code of element to count occurrences of
     * @return the estimated number
     */
    int frequency(int hashCode) {
        int hash = spread(hashCode);
        int start = (hash & 3) << 2;
        int frequency = Integer.MAX_VALUE;

        for (int i = 0; i < 4; i++) {
            int index = indexOf(hash, i);
            frequency = Math.min(frequency, (int) ((table[index] >>> ((start + i) << 2)) & 0xfL));
        }
        return frequency;
    }

    /**
     * Increments the popularity of the element if it does not exceed the maximum (15). The popularity of all
     * elements will be periodically down sampled when the observed events exceeds a threshold. This process
     * provides a frequency aging to allow expired long term entries to fade away.
     *
     * @param hashCode the hash code of element to add
     */
    void increment(int hashCode) {
        int hash = spread(hashCode);
        int start = (hash & 3) << 2;

        // Loop unrolling improves throughput by 5m ops/s
        int index0 = indexOf(hash, 0);
        int index1 = indexOf(hash, 1);
        int index2 = indexOf(hash, 2);
        int index3 = indexOf(hash, 3);

        boolean added = incrementAt(index0, start);
        added |= incrementAt(index1, start + 1);
        added |= incrementAt(index2, start + 2);
        added |= incrementAt(index3, start + 3);

        if (added && (++size == sampling)) {
            reset();
        }
    }

    /**
     * Increments the specified counter by 1 if it is not already at the maximum value (15).
     *
     * @param i the table index (16 counters)
     * @param j the counter to increment
     * @return if incremented
     */
    private boolean incrementAt(int i, int j) {
        int offset = j << 2;
        long mask = (0xfL << offset);
        if ((table[i] & mask) != mask) {
            table[i] += (1L << offset);
            return true;
        }
        return false;
    }

    /**
     * Reduces every counter by half of its original value.
     */
    private void reset() {
        int count = 0;
        for (int i = 0; i < table.length; i++) {
            count += Long.bitCount(table[i] & ONE_MASK);
            table[i] = (table[i] >>> 1) & RESET_MASK;
        }
        size = (size >>> 1) - (count >>> 2);
    }

    /**
     * Returns the table index for the counter at the specified depth.
     *
     * @param item the element's hash
     * @param i    the counter depth
     * @return the table index
     */
    private int indexOf(int item, int i) {
        long hash = (item + SEED[i]) * SEED[i];
        return ((int) (hash + (hash >>> 32))) & tableMask;
    }

    /**
     * Applies a supplemental hash function to a given hash code, which defends against poor quality hash
     * functions.
     *
     * @param x the hash code of element to supplement
     * @return supplemented hash code
     */
    private static int spread(int x) {
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        return (x >>> 16) ^ x;
    }
}
