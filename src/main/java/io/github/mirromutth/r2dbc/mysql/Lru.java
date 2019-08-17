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

import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.require;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * A minimal implementation of LRU without hashing map.
 * <p>
 * If want to use, should take with a external hash map.
 */
final class Lru<T> {

    @Nullable
    private Node<T> head;

    @Nullable
    private Node<T> tail;

    private int size;

    private final int limit;

    Lru(int limit) {
        require(limit > 0, "limit must be greater than 0");

        this.limit = limit;
    }

    void refresh(Node<T> node) {
        requireNonNull(node, "node must not be null");
        require(this == node.lru, "node must be an element of this LRU");

        if (node == head) {
            // Must be pointer compare.
            return;
        }

        Node<T> prev = node.prev, next = node.next, head = this.head;

        if (head == null) {
            throw new IllegalStateException("LRU must not be empty");
        }

        if (prev != null) {
            prev.next = next;
        }
        if (next != null) {
            next.prev = prev;
        }

        if (node == tail) {
            // Must be pointer compare.
            tail = prev;
        }

        node.prev = null;
        node.next = head;
        head.prev = node;
        this.head = node;
    }

    void remove(Node<T> node) {
        requireNonNull(node, "node must not be null");
        require(this == node.lru, "node must not be an element of this LRU");

        Node<T> prev = node.prev, next = node.next, head = this.head, tail = this.tail;

        if (head == null || tail == null) {
            throw new IllegalStateException("LRU must not be empty");
        }

        if (prev != null) {
            prev.next = next;
        }
        if (next != null) {
            next.prev = prev;
        }

        if (node == tail) {
            // Must be pointer compare.
            this.tail = prev;
        }
        if (node == head) {
            this.head = next;
        }

        --size;
        node.reInit();
    }

    @Nullable
    Node<T> push(Node<T> node) {
        requireNonNull(node, "node must not be null");

        node.lru = this;
        node.prev = null;
        node.next = head;

        if (head == null) {
            tail = node;
        } else {
            head.prev = node;
        }

        head = node;

        if (size < limit) {
            ++size;
            return null;
        } else {
            Node<T> tail = this.tail;

            if (tail == null || tail.prev == null) {
                throw new IllegalStateException("LRU must be contains least two elements when choose victim element");
            }
            this.tail = tail.prev;
            this.tail.next = null;

            tail.reInit();

            return tail;
        }
    }

    @Nullable
    T nextEviction() {
        if (size < limit || tail == null) {
            return null;
        } else {
            return tail.getValue();
        }
    }

    @Override
    public String toString() {
        Node<T> head = this.head, tail = this.tail;

        if (head == null || tail == null) {
            return "[]";
        }

        StringBuilder builder = new StringBuilder().append('[');

        for (Node<T> ptr = head; ptr != null && ptr != tail; ptr = ptr.next) {
            builder.append(ptr.value).append(',').append(' ');
        }

        return builder.append(tail.value).append(']').toString();
    }

    static final class Node<T> {

        private final T value;

        @Nullable
        private Lru<T> lru;

        @Nullable
        private Node<T> prev;

        @Nullable
        private Node<T> next;

        Node(T value) {
            this.value = value;
        }

        T getValue() {
            return value;
        }

        @Nullable
        Lru<T> getLru() {
            return lru;
        }

        private void reInit() {
            lru = null;
            prev = next = null;
        }
    }
}
