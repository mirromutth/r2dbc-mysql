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

import dev.miku.r2dbc.mysql.message.client.PreparedExecuteMessage;
import dev.miku.r2dbc.mysql.message.client.TextQueryMessage;

import java.util.Arrays;

/**
 * A collection of {@link Parameter} for one bind invocation of a parametrized statement.
 *
 * @see ParametrizedStatementSupport
 */
final class Binding {

    private static final Parameter[] EMPTY_VALUES = {};

    private final Parameter[] values;

    Binding(int length) {
        this.values = length == 0 ? EMPTY_VALUES : new Parameter[length];
    }

    /**
     * Add a {@link Parameter} to the binding.
     *
     * @param index the index of the {@link Parameter}
     * @param value the {@link Parameter} from {@link PrepareParametrizedStatement}
     */
    void add(int index, Parameter value) {
        if (index < 0 || index >= this.values.length) {
            throw new IndexOutOfBoundsException("index must not be a negative integer and less than " + this.values.length);
        }

        this.values[index] = value;
    }

    /**
     * Convert binding to a request message.
     *
     * @param statementId prepared statement identifier
     * @param immediate   use {@code true} if should be execute immediate, otherwise return a open cursor message
     * @return an execute message or open cursor message
     */
    PreparedExecuteMessage toExecuteMessage(int statementId, boolean immediate) {
        if (values.length == 0) {
            return new PreparedExecuteMessage(statementId, immediate, EMPTY_VALUES);
        }

        if (values[0] == null) {
            throw new IllegalStateException("Parameters has been used");
        }

        return new PreparedExecuteMessage(statementId, immediate, drainValues());
    }

    TextQueryMessage toTextMessage(Query query) {
        return TextQueryMessage.of(query, drainValues());
    }

    /**
     * Clear/release binding values.
     */
    void clear() {
        int size = this.values.length;
        for (int i = 0; i < size; ++i) {
            Parameter value = this.values[i];
            this.values[i] = null;

            if (value != null) {
                value.dispose();
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

    private Parameter[] drainValues() {
        Parameter[] results = new Parameter[this.values.length];

        System.arraycopy(this.values, 0, results, 0, this.values.length);
        Arrays.fill(this.values, null);

        return results;
    }
}
