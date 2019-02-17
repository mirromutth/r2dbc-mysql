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
 * Assertion library for {@code r2dbc-mysql} implementation.
 * <p>
 * P.S. should use a {@code AssertUtils} in package {@code r2dbc-common} after the package appears.
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
     * Checks that a specified integer is not negative and
     * throws a customized {@link IllegalArgumentException} if it is.
     *
     * @param x       the specified integer to check for negatively
     * @param message the detail message to be used in the event that an {@link IllegalArgumentException} is thrown
     * @return {@code x} if not negative
     * @throws IllegalArgumentException if {@code x} is negative
     */
    public static int requireNonNegative(int x, String message) {
        if (x < 0) {
            throw new IllegalArgumentException(message);
        }

        return x;
    }

    /**
     * Checks that a specified integer is positive and throws a
     * customized {@link IllegalArgumentException} if it is not.
     *
     * @param x       the specified integer to check for positively
     * @param message the detail message to be used in the event that an {@link IllegalArgumentException} is thrown
     * @return {@code x} if positive
     * @throws IllegalArgumentException if {@code x} is not positive
     */
    public static int requirePositive(int x, String message) {
        if (x <= 0) {
            throw new IllegalArgumentException(message);
        }

        return x;
    }
}
