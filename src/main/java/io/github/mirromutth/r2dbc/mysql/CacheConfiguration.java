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

import io.github.mirromutth.r2dbc.mysql.constant.CacheType;

/**
 * A configuration used to specify the behavior of a cache.
 */
final class CacheConfiguration {

    private final CacheType type;

    private final int size;

    CacheConfiguration(CacheType type, int size) {
        this.type = type;
        this.size = size;
    }

    public CacheType getType() {
        return type;
    }

    public int getSize() {
        return size;
    }
}
