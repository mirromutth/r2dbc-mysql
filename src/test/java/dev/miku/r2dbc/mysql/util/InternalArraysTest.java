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

package dev.miku.r2dbc.mysql.util;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link InternalArrays}.
 */
final class InternalArraysTest {

    @Test
    void asReadOnlyList() {
        assertThat(InternalArrays.asReadOnlyList(1, 2, 3, 4))
            .isEqualTo(Arrays.asList(1, 2, 3, 4))
            .isNotInstanceOf(Arrays.asList(1, 2, 3, 4).getClass());
        assertThat(InternalArrays.asReadOnlyList(1))
            .isEqualTo(Collections.singletonList(1))
            .isExactlyInstanceOf(Collections.singletonList(1).getClass());
        assertThat(InternalArrays.asReadOnlyList())
            .isEqualTo(Collections.emptyList())
            .isExactlyInstanceOf(Collections.emptyList().getClass());
    }

    @Test
    void toReadOnlyList() {
        Integer[] arr = new Integer[] { 1, 2, 3, 4 };

        ListAssert<Integer> listAssert = assertThat(InternalArrays.toReadOnlyList(arr))
            .isEqualTo(Arrays.asList(1, 2, 3, 4))
            .isNotInstanceOf(Arrays.asList(1, 2, 3, 4).getClass());

        Arrays.fill(arr, 6);

        listAssert.isEqualTo(Arrays.asList(1, 2, 3, 4));

        assertThat(InternalArrays.toReadOnlyList(1))
            .isEqualTo(Collections.singletonList(1))
            .isExactlyInstanceOf(Collections.singletonList(1).getClass());
        assertThat(InternalArrays.toReadOnlyList())
            .isEqualTo(Collections.emptyList())
            .isExactlyInstanceOf(Collections.emptyList().getClass());
    }
}
