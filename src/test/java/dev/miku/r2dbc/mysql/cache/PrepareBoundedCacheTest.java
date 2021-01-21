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

import org.junit.jupiter.api.Test;

import java.util.function.IntConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PrepareBoundedCache}.
 */
class PrepareBoundedCacheTest {

    private static final int DEFAULT_CAPACITY = 256;

    @Test
    void putIfAbsent() {
        PrepareBoundedCache cache = mock();

        assertThat(cache.toString()).isEqualTo("[][][]");
        assertThat(cache.putIfAbsent("SELECT 1", 1, ExceptionConsumer.INSTANCE)).isTrue();
        assertThat(cache.toString()).isEqualTo("[1][][]");
        assertThat(cache.putIfAbsent("SELECT 1", 2, ExceptionConsumer.INSTANCE)).isFalse();
        assertThat(cache.toString()).isEqualTo("[1][][]");
        assertThat(cache.getIfPresent("SELECT 1")).isEqualTo(1);
    }

    @Test
    void getIfPresent() {
        PrepareBoundedCache cache = mock();

        assertThat(cache.toString()).isEqualTo("[][][]");
        assertThat(cache.getIfPresent("SELECT 1")).isNull();
        assertThat(cache.putIfAbsent("SELECT 1", 1, ExceptionConsumer.INSTANCE)).isTrue();
        assertThat(cache.toString()).isEqualTo("[1][][]");
        assertThat(cache.getIfPresent("SELECT 1")).isEqualTo(1);
    }

    private static PrepareBoundedCache mock() {
        return new PrepareBoundedCache(DEFAULT_CAPACITY);
    }

    private static final class ExceptionConsumer implements IntConsumer {

        private static final ExceptionConsumer INSTANCE = new ExceptionConsumer();

        @Override
        public void accept(int value) {
            throw new RuntimeException("Unexpected value: " + value);
        }
    }
}
