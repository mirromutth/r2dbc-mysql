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

package io.github.mirromutth.r2dbc.mysql.converter;

import io.github.mirromutth.r2dbc.mysql.constant.ColumnType;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;

import java.lang.reflect.Type;

/**
 * Converter for primitive types, like {@link int} or {@link double}.
 */
abstract class AbstractPrimitiveConverter<T> implements Converter<T, Class<? super T>> {

    private final Class<T> primitiveClass;

    private final Class<T> boxedClass;

    AbstractPrimitiveConverter(Class<T> primitiveClass, Class<T> boxedClass) {
        if (!primitiveClass.isPrimitive() || boxedClass.isPrimitive()) {
            throw new IllegalArgumentException("primitiveClass must be primitive and boxedClass must not be primitive");
        }

        this.primitiveClass = primitiveClass;
        this.boxedClass = boxedClass;
    }

    @Override
    public final boolean canRead(ColumnType type, boolean isUnsigned, int precision, int collationId, Type target, MySqlSession session) {
        if (!(target instanceof Class<?>)) {
            return false;
        }

        return (primitiveClass == target || ((Class<?>) target).isAssignableFrom(boxedClass)) && doCanRead(type, isUnsigned, precision);
    }

    abstract boolean doCanRead(ColumnType type, boolean isUnsigned, int precision);
}
