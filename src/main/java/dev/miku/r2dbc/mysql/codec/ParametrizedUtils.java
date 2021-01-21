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

import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * An utility for extract {@link ParameterizedType} raw type and type arguments with checking.
 */
final class ParametrizedUtils {

    @Nullable
    static Class<?> getTypeArgument(ParameterizedType type, Class<?> rawClass) {
        Type[] arguments = type.getActualTypeArguments();

        if (arguments.length != 1) {
            return null;
        }

        Type result = arguments[0];

        return type.getRawType() == rawClass && result instanceof Class<?> ? (Class<?>) result : null;
    }

    private ParametrizedUtils() { }
}
