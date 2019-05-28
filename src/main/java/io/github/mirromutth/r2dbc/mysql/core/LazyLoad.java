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

import reactor.util.annotation.NonNull;

import java.util.function.Supplier;

/**
 * Lazy loading and caching values ​​for multiple acquisitions,
 * implementations ensure that the value is only loaded once.
 *
 * Can NOT return a null
 * <p>
 * Implementations are thread safety.
 */
@FunctionalInterface
public interface LazyLoad<T> extends Supplier<T> {

    @NonNull
    @Override
    T get();

    static <T> LazyLoad<T> of(Supplier<T> supplier) {
        return new SyncLazyLoad<>(supplier);
    }
}
