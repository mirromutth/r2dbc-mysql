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

package dev.miku.r2dbc.mysql.codec;

import io.netty.buffer.ByteBufAllocator;

/**
 * Codec for classed type when field bytes less or equals than {@link Integer#MAX_VALUE}.
 *
 * @param <T> the type of handling data.
 */
abstract class AbstractClassedCodec<T> implements Codec<T> {

    protected final ByteBufAllocator allocator;

    private final Class<? extends T> clazz;

    AbstractClassedCodec(ByteBufAllocator allocator, Class<? extends T> clazz) {
        this.allocator = allocator;
        this.clazz = clazz;
    }

    @Override
    public final boolean canDecode(FieldInformation info, Class<?> target) {
        return target.isAssignableFrom(this.clazz) && doCanDecode(info);
    }

    abstract protected boolean doCanDecode(FieldInformation info);
}
