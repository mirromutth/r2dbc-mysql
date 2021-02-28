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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.MySqlColumnMetadata;
import io.netty.buffer.ByteBufAllocator;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * Codec for primitive types, like {@code int} or {@code double}.
 *
 * @param <T> the boxed type of handling primitive data.
 */
abstract class AbstractPrimitiveCodec<T> implements PrimitiveCodec<T> {

    protected final ByteBufAllocator allocator;

    private final Class<T> primitiveClass;

    private final Class<T> boxedClass;

    AbstractPrimitiveCodec(ByteBufAllocator allocator, Class<T> primitiveClass, Class<T> boxedClass) {
        require(primitiveClass.isPrimitive() && !boxedClass.isPrimitive(),
            "primitiveClass must be primitive and boxedClass must not be primitive");

        this.allocator = allocator;
        this.primitiveClass = primitiveClass;
        this.boxedClass = boxedClass;
    }

    @Override
    public final boolean canDecode(MySqlColumnMetadata metadata, Class<?> target) {
        return target.isAssignableFrom(boxedClass) && doCanDecode(metadata);
    }

    @Override
    public final boolean canPrimitiveDecode(MySqlColumnMetadata metadata) {
        return doCanDecode(metadata);
    }

    @Override
    public final Class<T> getPrimitiveClass() {
        return primitiveClass;
    }

    abstract protected boolean doCanDecode(MySqlColumnMetadata metadata);
}
