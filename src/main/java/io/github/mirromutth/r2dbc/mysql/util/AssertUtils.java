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

package io.github.mirromutth.r2dbc.mysql.util;

import reactor.util.annotation.Nullable;

/**
 * Assertion library for {@literal r2dbc-mysql} implementation.
 */
public final class AssertUtils {

    private AssertUtils() {
    }

    /**
     * Checks that a specified object reference is not {@code null} and
     * throws a customized {@link IllegalArgumentException} if it is.
     *
     * @param obj     the object reference to check for nullity
     * @param message the detail message to be used in the event that an {@link IllegalArgumentException} is thrown
     * @param <T>     the type of the reference
     * @return {@code obj} if not {@code null}
     * @throws IllegalArgumentException if {@code obj} is {@code null}
     */
    public static <T> T requireNonNull(@Nullable T obj, String message) {
        if (obj == null) {
            throw new IllegalArgumentException(message);
        }

        return obj;
    }

    /**
     * Checks that a specified condition is {@code true} and throws a
     * customized {@link IllegalArgumentException} if it is not.
     *
     * @param condition the condition value
     * @param message   the detail message to be used in the event that an {@link IllegalArgumentException} is thrown
     * @throws IllegalArgumentException if {@code condition} is {@code false}.
     */
    public static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Checks that a specified {@link String} is not {@code null} or empty or backticks included and
     * throws a customized {@link IllegalArgumentException} if it is.
     *
     * @param name    the {@link String} to check for nullity or empty or backticks included
     * @param message the detail message to be used in the event that an {@link IllegalArgumentException} is thrown
     * @return {@code name} if not {@code null} or empty or backticks included
     * @throws IllegalArgumentException if {@code name} is {@code null} or empty or backticks included
     */
    public static String requireValidName(@Nullable String name, String message) {
        if (name == null || name.isEmpty() || name.indexOf('`') >= 0) {
            throw new IllegalArgumentException(message);
        }

        return name;
    }
}
