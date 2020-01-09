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

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Base class considers parametrized {@link MySqlStatement} with parameter markers.
 * <p>
 * MySQL uses indexed parameters which are marked by {@literal ?} without naming.
 * Implementations should uses {@link Query} to supports named parameters.
 */
abstract class ParametrizedStatementSupport extends MySqlStatementSupport {

    protected final Client client;

    protected final Codecs codecs;

    protected final ConnectionContext context;

    private final Bindings bindings;

    private final AtomicBoolean executed = new AtomicBoolean();

    ParametrizedStatementSupport(Client client, Codecs codecs, ConnectionContext context, int parameters) {
        require(parameters > 0, "parameters must be a positive integer");

        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.context = requireNonNull(context, "context must not be null");
        this.bindings = new Bindings(parameters);
    }

    @Override
    public final MySqlStatement add() {
        assertNotExecuted();

        this.bindings.validatedFinish();
        return this;
    }

    @Override
    public final MySqlStatement bind(int index, Object value) {
        requireNonNull(value, "value must not be null");

        addBinding(index, codecs.encode(value, context));
        return this;
    }

    @Override
    public final MySqlStatement bind(String name, Object value) {
        requireNonNull(name, "name must not be null");
        requireNonNull(value, "value must not be null");

        addBinding(getIndexes(name), codecs.encode(value, context));
        return this;
    }

    @Override
    public final MySqlStatement bindNull(int index, Class<?> type) {
        // Useless, but should be checked in here, for programming robustness
        requireNonNull(type, "type must not be null");

        addBinding(index, codecs.encodeNull());
        return this;
    }

    @Override
    public final MySqlStatement bindNull(String name, Class<?> type) {
        requireNonNull(name, "name must not be null");
        // Useless, but should be checked in here, for programming robustness
        requireNonNull(type, "type must not be null");

        addBinding(getIndexes(name), codecs.encodeNull());
        return this;
    }

    @Override
    public final Flux<MySqlResult> execute() {
        if (bindings.bindings.isEmpty()) {
            throw new IllegalStateException("No parameters bound for current statement");
        }
        bindings.validatedFinish();

        return Flux.defer(() -> {
            if (!executed.compareAndSet(false, true)) {
                return Flux.error(new IllegalStateException("Parametrized statement was already executed"));
            }

            return execute(bindings.bindings);
        });
    }

    abstract protected Flux<MySqlResult> execute(List<Binding> bindings);

    /**
     * Get parameter index(es) by parameter name.
     *
     * @param name the parameter name
     * @return the {@link ParameterIndex} including an index or multi-indexes
     * @throws IllegalArgumentException if parameter {@code name} not found
     */
    abstract protected ParameterIndex getIndexes(String name);

    private void addBinding(int index, ParameterValue value) {
        assertNotExecuted();

        this.bindings.getCurrent().add(index, value);
    }

    private void addBinding(ParameterIndex indexes, ParameterValue value) {
        assertNotExecuted();

        indexes.bind(this.bindings.getCurrent(), value);
    }

    private void assertNotExecuted() {
        if (this.executed.get()) {
            throw new IllegalStateException("Statement was already executed");
        }
    }

    private static final class Bindings implements Iterable<Binding> {

        private final List<Binding> bindings = new ArrayList<>();

        private final int paramCount;

        private Binding current;

        private Bindings(int paramCount) {
            this.paramCount = paramCount;
        }

        @Override
        public Iterator<Binding> iterator() {
            return bindings.iterator();
        }

        @Override
        public void forEach(Consumer<? super Binding> action) {
            bindings.forEach(action);
        }

        @Override
        public Spliterator<Binding> spliterator() {
            return bindings.spliterator();
        }

        private void validatedFinish() {
            Binding current = this.current;

            if (current == null) {
                return;
            }

            int unbind = current.findUnbind();

            if (unbind >= 0) {
                String message = String.format("Parameter %d has no binding", unbind);
                throw new IllegalStateException(message);
            }

            this.current = null;
        }

        private Binding getCurrent() {
            Binding current = this.current;

            if (current == null) {
                current = new Binding(this.paramCount);
                this.current = current;
                this.bindings.add(current);
            }

            return current;
        }
    }
}
