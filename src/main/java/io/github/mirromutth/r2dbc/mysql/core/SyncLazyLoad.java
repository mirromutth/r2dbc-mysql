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

package io.github.mirromutth.r2dbc.mysql.core;

import java.util.function.Supplier;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link LazyLoad} which based on {@code synchronized}.
 */
final class SyncLazyLoad<T> implements LazyLoad<T> {

    private volatile T value = null;

    private Supplier<T> supplier;

    SyncLazyLoad(Supplier<T> supplier) {
        this.supplier = requireNonNull(supplier, "supplier must not be null");
    }

    @Override
    public T get() {
        if (this.value == null) {
            synchronized (this) {
                if (this.value == null) {
                    T value = requireNonNull(this.supplier.get(), "value must not be null");
                    this.supplier = null;
                    this.value = value;
                    return value;
                }

                return this.value;
            }
        }

        return this.value;
    }
}
