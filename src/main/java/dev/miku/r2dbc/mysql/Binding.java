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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.PreparedExecuteMessage;

import java.util.Arrays;

/**
 * A collection of {@link ParameterValue} for one bind invocation of a prepared statement.
 * Bindings for MySQL are index-based.
 *
 * @see ParametrizedMySqlStatement
 */
final class Binding {

    private final ParameterValue[] values;

    Binding(int length) {
        this.values = new ParameterValue[length];
    }

    /**
     * Add a {@link ParameterValue} to the binding.
     *
     * @param index the index of the {@link ParameterValue}
     * @param value the {@link ParameterValue} from {@link ParametrizedMySqlStatement}
     */
    void add(int index, ParameterValue value) {
        if (index < 0 || index >= this.values.length) {
            throw new IndexOutOfBoundsException("index must not be a negative integer and less than " + this.values.length);
        }

        this.values[index] = value;
    }

    PreparedExecuteMessage toMessage(int statementId) {
        ParameterValue[] values = new ParameterValue[this.values.length];

        System.arraycopy(this.values, 0, values, 0, this.values.length);
        Arrays.fill(this.values, null);

        return new PreparedExecuteMessage(statementId, values);
    }

    /**
     * Clear/release binding values.
     */
    void clear() {
        int size = this.values.length;
        for (int i = 0; i < size; ++i) {
            if (this.values[i] != null) {
                this.values[i].dispose();
                this.values[i] = null;
            }
        }
    }

    int findUnbind() {
        int size = this.values.length;

        for (int i = 0; i < size; ++i) {
            if (this.values[i] == null) {
                return i;
            }
        }

        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Binding)) {
            return false;
        }

        Binding binding = (Binding) o;

        return Arrays.equals(this.values, binding.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return String.format("Binding{values=%s}", Arrays.toString(values));
    }
}
