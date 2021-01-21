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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Lru}.
 */
class LruTest {

    private static final int LRU_SIZE = 3;

    @Test
    void refresh() {
        Lru<Integer> list = mockLru();
        Lru.Node<Integer> one = new Lru.Node<>("1", 1);

        assertThat(list.push(one)).isNull();
        assertThat(list.toString()).isEqualTo("[1]");
        list.refresh(one);
        assertThat(list.toString()).isEqualTo("[1]");

        Lru.Node<Integer> two = new Lru.Node<>("2", 2);

        assertThat(list.push(two)).isNull();
        assertThat(list.toString()).isEqualTo("[2, 1]");
        list.refresh(one);
        assertThat(list.toString()).isEqualTo("[1, 2]");
        list.refresh(two);
        assertThat(list.toString()).isEqualTo("[2, 1]");

        Lru.Node<Integer> three = new Lru.Node<>("3", 3);

        assertThat(list.push(three)).isNull();
        assertThat(list.toString()).isEqualTo("[3, 2, 1]");
        list.refresh(two);
        assertThat(list.toString()).isEqualTo("[2, 3, 1]");
        list.refresh(one);
        assertThat(list.toString()).isEqualTo("[1, 2, 3]");
        list.refresh(two);
        assertThat(list.toString()).isEqualTo("[2, 1, 3]");
        list.refresh(three);
        assertThat(list.toString()).isEqualTo("[3, 2, 1]");
        list.refresh(three);
        assertThat(list.toString()).isEqualTo("[3, 2, 1]");

        assertThat(list.push(new Lru.Node<>("4", 4)))
            .isNotNull()
            .extracting(Lru.Node::getValue)
            .isEqualTo(1);

        assertThat(list.toString()).isEqualTo("[4, 3, 2]");
    }

    @Test
    void push() {
        Lru<Integer> list = mockLru();

        assertThat(list.toString()).isEqualTo("[]");
        assertThat(list.push(new Lru.Node<>("1", 1))).isNull();
        assertThat(list.toString()).isEqualTo("[1]");
        assertThat(list.push(new Lru.Node<>("2", 2))).isNull();
        assertThat(list.toString()).isEqualTo("[2, 1]");
        assertThat(list.push(new Lru.Node<>("3", 3))).isNull();
        assertThat(list.toString()).isEqualTo("[3, 2, 1]");

        assertThat(list.push(new Lru.Node<>("4", 4)))
            .isNotNull()
            .extracting(Lru.Node::getValue)
            .isEqualTo(1);

        assertThat(list.toString()).isEqualTo("[4, 3, 2]");
    }

    private static <T> Lru<T> mockLru() {
        return new Lru<>(LRU_SIZE, 1);
    }
}
