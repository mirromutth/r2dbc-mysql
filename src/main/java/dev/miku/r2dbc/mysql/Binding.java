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
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * A collection of {@link ParameterValue} for one bind invocation of a parametrized statement.
 *
 * @see ParametrizedStatementSupport
 */
final class Binding {

    private static final ParameterValue[] EMPTY_VALUES = {};

    private final ParameterValue[] values;

    Binding(int length) {
        this.values = length == 0 ? EMPTY_VALUES : new ParameterValue[length];
    }

    /**
     * Add a {@link ParameterValue} to the binding.
     *
     * @param index the index of the {@link ParameterValue}
     * @param value the {@link ParameterValue} from {@link PrepareParametrizedStatement}
     */
    void add(int index, ParameterValue value) {
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
    PreparedExecuteMessage toMessage(int statementId, boolean immediate) {
        if (values.length == 0) {
            return new PreparedExecuteMessage(statementId, immediate, EMPTY_VALUES);
        }

        if (values[0] == null) {
            throw new IllegalStateException("Parameters has been used");
        }

        return new PreparedExecuteMessage(statementId, immediate, drainValues());
    }

    Mono<String> toSql(List<String> sqlParts) {
        // No need defer, because it must be called by inner of Publisher processing.
        int size = sqlParts.size() - 1;

        if (values.length != size || size == 0) {
            clear();
            return Mono.error(new IllegalArgumentException("The number of parameter should be " + size + ", but was " + values.length));
        }

        if (values[0] == null) {
            return Mono.error(new IllegalStateException("Parameters has been used"));
        }

        try {
            StringBuilder builder = new StringBuilder();
            Iterator<String> iter = sqlParts.iterator();
            Consumer<Object> step = ignored -> builder.append(iter.next());

            builder.append(iter.next());

            return OperatorUtils.discardOnCancel(Flux.fromArray(drainValues()))
                .doOnDiscard(ParameterValue.class, ParameterValue.DISPOSE)
                .concatMap(it -> it.writeTo(builder).doOnSuccess(step))
                .then(Mono.fromSupplier(builder::toString));
        } catch (Throwable e) {
            clear();
            throw e;
        }
    }

    /**
     * Clear/release binding values.
     */
    void clear() {
        int size = this.values.length;
        for (int i = 0; i < size; ++i) {
            ParameterValue value = this.values[i];
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

    private ParameterValue[] drainValues() {
        ParameterValue[] results = new ParameterValue[this.values.length];

        System.arraycopy(this.values, 0, results, 0, this.values.length);
        Arrays.fill(this.values, null);

        return results;
    }

    static void clearSubsequent(Iterator<Binding> iterator) {
        while (iterator.hasNext()) {
            iterator.next().clear();
        }
    }
}
