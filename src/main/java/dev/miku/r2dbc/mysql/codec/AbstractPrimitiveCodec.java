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

import java.lang.reflect.Type;

/**
 * Codec for primitive types, like {@link int} or {@link double}.
 */
abstract class AbstractPrimitiveCodec<T> implements PrimitiveCodec<T> {

    private final Class<T> primitiveClass;

    private final Class<T> boxedClass;

    AbstractPrimitiveCodec(Class<T> primitiveClass, Class<T> boxedClass) {
        if (!primitiveClass.isPrimitive() || boxedClass.isPrimitive()) {
            throw new IllegalArgumentException("primitiveClass must be primitive and boxedClass must not be primitive");
        }

        this.primitiveClass = primitiveClass;
        this.boxedClass = boxedClass;
    }

    @Override
    public final boolean canDecode(boolean massive, FieldInformation info, Type target) {
        if (!(target instanceof Class<?>) || massive) {
            return false;
        }

        return ((Class<?>) target).isAssignableFrom(boxedClass) && doCanDecode(info);
    }

    @Override
    public final boolean canPrimitiveDecode(FieldInformation info) {
        return doCanDecode(info);
    }

    @Override
    public final Class<T> getPrimitiveClass() {
        return primitiveClass;
    }

    abstract protected boolean doCanDecode(FieldInformation info);
}
