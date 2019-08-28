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

/**
 * A minimal implementation of Segmented LRU without hashing map.
 * <p>
 * If want to use, should take with a external hash map.
 */
final class Slru<T> {

    private final Lru<T> protection;

    private final Lru<T> probation;

    Slru(int capability) {
        int size = (int) (capability * 0.8);

        this.protection = new Lru<>(Math.max(1, size));
        this.probation = new Lru<>(Math.max(1, capability - size));
    }

    void refresh(Lru.Node<T> node) {
        Lru<T> lru = node.getLru();

        if (lru == protection) {
            protection.refresh(node);
        } else if (lru == probation) {
            probation.remove(node);
            Lru.Node<T> evicted = protection.push(node);

            if (evicted != null) {
                // This element must be protect and must not be really victim.
                // Must return null because probation has removed one element.
                probation.push(evicted);
            }
        } else {
            throw new IllegalArgumentException("node must be an element of this SLRU");
        }
    }

    @Nullable
    Lru.Node<T> push(Lru.Node<T> node) {
        return probation.push(node);
    }

    @Nullable
    Lru.Node<T> nextEviction() {
        return probation.nextEviction();
    }

    @Override
    public String toString() {
        return String.format("[%s, %s]", protection, probation);
    }
}
