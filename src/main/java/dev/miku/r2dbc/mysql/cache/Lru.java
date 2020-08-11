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

import reactor.util.annotation.Nullable;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * A minimal implementation of LRU without hashing map.
 * <p>
 * If want to use, should take with a external hash map.
 */
final class Lru<T> {

    static final int WINDOW = 1;

    static final int PROBATION = 2;

    static final int PROTECTION = 3;

    private final int limit;

    private final int eigenvalue;

    @Nullable
    private Node<T> head;

    @Nullable
    private Node<T> tail;

    private int size;

    Lru(int limit, int eigenvalue) {
        require(limit > 0, "limit must be greater than 0");
        require(eigenvalue > 0, "eigenvalue must be greater than 0");

        this.limit = limit;
        this.eigenvalue = eigenvalue;
    }

    void refresh(Node<T> node) {
        Node<T> head = this.head;

        if (node == head) {
            // No need refresh.
            return;
        }

        Node<T> prev = node.prev, next = node.next;

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
            this.tail = prev;
        }

        node.prev = null;
        node.next = head;
        head.prev = node;
        this.head = node;
    }

    @Nullable
    Node<T> push(Node<T> node) {
        Node<T> head = this.head;

        node.lru = this.eigenvalue;
        node.prev = null;
        node.next = head;

        if (head == null) {
            this.tail = node;
        } else {
            head.prev = node;
        }

        this.head = node;

        if (size < limit) {
            ++size;
            return null;
        }

        Node<T> unlink = this.tail;
        Node<T> tail;

        if (unlink == null || (tail = unlink.prev) == null) {
            throw new IllegalStateException("LRU must be contains least 2 elements when choose an unlink element");
        }

        tail.next = null;
        this.tail = tail;

        unlink.reInit();

        return unlink;
    }

    @Nullable
    Lru.Node<T> nextEviction() {
        return size < limit ? null : tail;
    }

    void remove(Node<T> node) {
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

        private final String key;

        private final T value;

        private int lru;

        @Nullable
        private Node<T> prev;

        @Nullable
        private Node<T> next;

        Node(String key, T value) {
            this.key = key;
            this.value = value;
        }

        int getLru() {
            return lru;
        }

        String getKey() {
            return key;
        }

        T getValue() {
            return value;
        }

        private void reInit() {
            lru = 0;
            prev = next = null;
        }
    }
}
