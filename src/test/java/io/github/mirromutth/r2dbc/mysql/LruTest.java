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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link Lru}.
 */
class LruTest {

    @Test
    void refresh() {
        Lru<Integer> list = new Lru<>(3);
        Lru.Node<Integer> one = new Lru.Node<>(1);
        assertNull(list.push(one));
        list.refresh(one);
        assertEquals(list.toString(), Collections.singletonList(1).toString());
        Lru.Node<Integer> two = new Lru.Node<>(2);
        assertNull(list.push(two));
        list.refresh(one);
        assertEquals(list.toString(), Arrays.asList(1, 2).toString());
        list.refresh(two);
        assertEquals(list.toString(), Arrays.asList(2, 1).toString());
        Lru.Node<Integer> three = new Lru.Node<>(3);
        assertNull(list.push(three));
        list.refresh(two);
        assertEquals(list.toString(), Arrays.asList(2, 3, 1).toString());
        list.refresh(one);
        assertEquals(list.toString(), Arrays.asList(1, 2, 3).toString());
        list.refresh(two);
        assertEquals(list.toString(), Arrays.asList(2, 1, 3).toString());
        list.refresh(three);
        assertEquals(list.toString(), Arrays.asList(3, 2, 1).toString());
        list.refresh(three);
        assertEquals(list.toString(), Arrays.asList(3, 2, 1).toString());
        Lru.Node<Integer> victim = list.push(new Lru.Node<>(4));
        assertNotNull(victim);
        assertEquals(victim.getValue(), 1);
        assertEquals(list.toString(), Arrays.asList(4, 3, 2).toString());
    }

    @Test
    void push() {
        Lru<Integer> list = new Lru<>(3);
        assertEquals(list.toString(), Collections.emptyList().toString());
        assertNull(list.push(new Lru.Node<>(1)));
        assertEquals(list.toString(), Collections.singletonList(1).toString());
        assertNull(list.push(new Lru.Node<>(2)));
        assertEquals(list.toString(), Arrays.asList(2, 1).toString());
        assertNull(list.push(new Lru.Node<>(3)));
        assertEquals(list.toString(), Arrays.asList(3, 2, 1).toString());
        Lru.Node<Integer> victim = list.push(new Lru.Node<>(4));
        assertNotNull(victim);
        assertEquals(victim.getValue(), 1);
        assertEquals(list.toString(), Arrays.asList(4, 3, 2).toString());
    }
}
