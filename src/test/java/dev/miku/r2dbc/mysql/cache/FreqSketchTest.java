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

import org.assertj.core.api.ThrowableTypeAssert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link FreqSketch}.
 */
class FreqSketchTest {

    private final int hashCode = ThreadLocalRandom.current().nextInt();

    @Test
    void badConstruct() {
        ThrowableTypeAssert<IllegalArgumentException> typeAssert = assertThatIllegalArgumentException();

        typeAssert.isThrownBy(() -> new FreqSketch(0));
        typeAssert.isThrownBy(() -> new FreqSketch(-1));
        typeAssert.isThrownBy(() -> new FreqSketch(Integer.MIN_VALUE));
        typeAssert.isThrownBy(() -> new FreqSketch(Integer.MIN_VALUE >> 1));
    }

    @Test
    void incrementOnce() {
        FreqSketch sketch = new FreqSketch(1);

        sketch.increment(hashCode);
        assertThat(sketch.frequency(hashCode)).isEqualTo(1);
    }

    @Test
    void incrementMax() {
        FreqSketch sketch = new FreqSketch(256);

        for (int i = 0; i < 20; i++) {
            sketch.increment(hashCode);
        }

        assertThat(sketch.frequency(hashCode)).isEqualTo(15);
    }

    @Test
    void incrementDistinct() {
        FreqSketch sketch = new FreqSketch(256);

        sketch.increment(hashCode);
        sketch.increment(hashCode + 1);
        assertThat(sketch.frequency(hashCode)).isEqualTo(1);
        assertThat(sketch.frequency(hashCode + 1)).isEqualTo(1);
        assertThat(sketch.frequency(hashCode + 2)).isEqualTo(0);
    }

    @Test
    void indexOfAroundZero() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method indexOf = FreqSketch.class.getDeclaredMethod("indexOf", int.class, int.class);
        FreqSketch sketch = new FreqSketch(256);
        Set<Integer> indexes = new HashSet<>(16);
        int[] hashes = { -1, 0, 1 };

        indexOf.setAccessible(true);

        for (int hash : hashes) {
            for (int i = 0; i < 4; i++) {
                indexes.add((Integer) indexOf.invoke(sketch, hash, i));
            }
        }

        assertThat(indexes).hasSize(hashes.length << 2);
    }

    @Test
    void reset() throws NoSuchFieldException, IllegalAccessException {
        boolean reset = false;
        Field table = FreqSketch.class.getDeclaredField("table");
        Field size = FreqSketch.class.getDeclaredField("size");
        Field sampling = FreqSketch.class.getDeclaredField("sampling");
        FreqSketch sketch = new FreqSketch(64);

        table.setAccessible(true);
        size.setAccessible(true);
        sampling.setAccessible(true);

        int length = ((long[]) table.get(sketch)).length * 20;

        for (int i = 1; i < length; i++) {
            sketch.increment(i);
            if (size.getInt(sketch) != i) {
                reset = true;
                break;
            }
        }
        assertThat(reset).isTrue();
        assertThat(size.getInt(sketch)).isLessThanOrEqualTo(sampling.getInt(sketch) >>> 1);
    }

    @Test
    public void heavyHitters() {
        FreqSketch sketch = new FreqSketch(256);
        for (int i = 100; i < 100_000; i++) {
            sketch.increment(Double.hashCode(i));
        }
        for (int i = 0; i < 10; i += 2) {
            for (int j = 0; j < i; j++) {
                sketch.increment(Double.hashCode(i));
            }
        }

        // A perfect popularity count yields an array [0, 0, 2, 0, 4, 0, 6, 0, 8, 0]
        int[] popularity = new int[10];
        for (int i = 0; i < 10; i++) {
            popularity[i] = sketch.frequency(Double.hashCode(i));
        }
        for (int i = 0; i < popularity.length; i++) {
            if ((i == 0) || (i == 1) || (i == 3) || (i == 5) || (i == 7) || (i == 9)) {
                assertThat(popularity[i]).isLessThanOrEqualTo(popularity[2]);
            } else if (i == 2) {
                assertThat(popularity[2]).isLessThanOrEqualTo(popularity[4]);
            } else if (i == 4) {
                assertThat(popularity[4]).isLessThanOrEqualTo(popularity[6]);
            } else if (i == 6) {
                assertThat(popularity[6]).isLessThanOrEqualTo(popularity[8]);
            }
        }
    }
}
