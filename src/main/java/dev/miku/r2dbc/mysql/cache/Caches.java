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

/**
 * An utility for create caches from configuration.
 */
public final class Caches {

    /**
     * Create a new {@link QueryCache} by cache configuration.
     *
     * @param capacity the capacity of {@link QueryCache}.
     * @return the above {@link QueryCache}.
     */
    public static QueryCache createQueryCache(int capacity) {
        if (capacity > 0 && capacity < Integer.MAX_VALUE) {
            return new QueryBoundedCache(capacity);
        } else if (capacity == 0) {
            return new QueryDisabledCache();
        } else {
            return new QueryUnboundedCache();
        }
    }

    /**
     * Create a new {@link PrepareCache} by cache configuration.
     *
     * @param capacity the capacity of {@link PrepareCache}.
     * @return the above {@link PrepareCache}.
     */
    public static PrepareCache createPrepareCache(int capacity) {
        if (capacity > 0 && capacity < Integer.MAX_VALUE) {
            return new PrepareBoundedCache(capacity);
        } else if (capacity == 0) {
            return new PrepareDisabledCache();
        } else {
            return new PrepareUnboundedCache();
        }
    }

    /**
     * Returns the smallest power of two greater than or equal to {@code x}. This
     * is equivalent to {@code pow(2, ceil(log2(x)))}.
     * <p>
     * From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
     *
     * @param x between 0 and 1073741824 (inclusive).
     * @return the closest power-of-two at or higher than the given value {@code x}.
     */
    static int ceilingPowerOfTwo(int x) {
        return 1 << -Integer.numberOfLeadingZeros(x - 1);
    }
}
