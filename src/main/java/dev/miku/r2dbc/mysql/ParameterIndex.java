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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.message.ParameterValue;
import reactor.util.annotation.Nullable;

import java.util.Arrays;

/**
 * A data class considers indexes of a named parameter. Most case of the
 * parameter name to index relation is one-to-one, but one-to-many is
 * possible.
 */
final class ParameterIndex {

    private static final int INIT_CAPACITY = 4;

    private final int first;

    @Nullable
    private int[] values;

    private int size = 1;

    ParameterIndex(int first) {
        this.first = first;
    }

    void push(int value) {
        if (values == null) {
            int[] data = new int[INIT_CAPACITY];

            data[0] = first;
            data[1] = value;

            this.values = data;
            this.size = 2;
        } else {
            int i = this.size++;

            if (i >= values.length) {
                int[] data = new int[values.length << 1];
                System.arraycopy(values, 0, data, 0, values.length);
                this.values = data;
            }

            this.values[i] = value;
        }
    }

    void bind(Binding binding, ParameterValue value) {
        if (values == null) {
            binding.add(first, value);
        } else {
            for (int i = 0; i < size; ++i) {
                binding.add(values[i], value);
            }
        }
    }

    /**
     * Used by unit tests.
     */
    int[] toIntArray() {
        if (values == null) {
            return new int[]{first};
        } else {
            return Arrays.copyOf(values, size);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ParameterIndex)) {
            return false;
        }

        ParameterIndex that = (ParameterIndex) o;

        if (first != that.first) {
            return false;
        }
        if (size != that.size) {
            return false;
        }
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + Arrays.hashCode(values);
        result = 31 * result + size;
        return result;
    }

    @Override
    public String toString() {
        if (values == null) {
            return Integer.toString(first);
        }

        StringBuilder builder = new StringBuilder()
            .append('[')
            .append(values[0]);

        for (int i = 1; i < size; ++i) {
            builder.append(", ")
                .append(values[i]);
        }

        return builder.append(']').toString();
    }
}
