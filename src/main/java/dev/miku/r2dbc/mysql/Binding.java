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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.message.client.PreparedExecuteMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedTextQueryMessage;

import java.util.Arrays;

/**
 * A collection of {@link MySqlParameter} for one bind invocation of a parametrized statement.
 *
 * @see ParametrizedStatementSupport
 */
final class Binding {

    private static final MySqlParameter[] EMPTY_VALUES = { };

    private final MySqlParameter[] values;

    Binding(int length) {
        this.values = length == 0 ? EMPTY_VALUES : new MySqlParameter[length];
    }

    /**
     * Add a {@link MySqlParameter} to the binding.
     *
     * @param index the index of the {@link MySqlParameter}
     * @param value the {@link MySqlParameter} from {@link PrepareParametrizedStatement}
     */
    void add(int index, MySqlParameter value) {
        if (index < 0 || index >= this.values.length) {
            throw new IndexOutOfBoundsException("Index: " + index + ", length: " + this.values.length);
        }

        this.values[index] = value;
    }

    /**
     * Converts bindings to a request message. If not need execute immediate, it will return a open cursor
     * message.
     *
     * @param statementId prepared statement identifier.
     * @param immediate   if should be execute immediate, otherwise return a open cursor message
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

    PreparedTextQueryMessage toTextMessage(Query query) {
        return new PreparedTextQueryMessage(query, drainValues());
    }

    /**
     * Clear/release binding values.
     */
    void clear() {
        int size = this.values.length;
        for (int i = 0; i < size; ++i) {
            MySqlParameter value = this.values[i];
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

    private MySqlParameter[] drainValues() {
        MySqlParameter[] results = new MySqlParameter[this.values.length];

        System.arraycopy(this.values, 0, results, 0, this.values.length);
        Arrays.fill(this.values, null);

        return results;
    }
}
