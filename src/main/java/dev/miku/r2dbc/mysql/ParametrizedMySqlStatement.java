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
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Parametrized {@link MySqlStatement} with parameter markers executed against a Microsoft SQL Server database.
 * <p>
 * MySQL uses indexed parameters which are marked by {@literal ?} without naming. This implementation uses
 * {@link Query} to implement named parameters, and different indexes can have the same name.
 */
final class ParametrizedMySqlStatement extends MySqlStatementSupport {

    private final Client client;

    private final Codecs codecs;

    private final ConnectionContext context;

    private final Query query;

    private final Bindings bindings;

    private final AtomicBoolean executed = new AtomicBoolean();

    ParametrizedMySqlStatement(Client client, Codecs codecs, ConnectionContext context, Query query) {
        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.context = requireNonNull(context, "context must not be null");
        this.query = requireNonNull(query, "sql must not be null");
        this.bindings = new Bindings(this.query.getParameters());
    }

    @Override
    public MySqlStatement add() {
        assertNotExecuted();

        this.bindings.validatedFinish();
        return this;
    }

    @Override
    public MySqlStatement bind(int index, Object value) {
        requireNonNull(value, "value must not be null");

        addBinding(index, codecs.encode(value, context));
        return this;
    }

    @Override
    public MySqlStatement bind(String name, Object value) {
        requireNonNull(name, "name must not be null");
        requireNonNull(value, "value must not be null");

        Object indexes = query.getIndexes(name);

        if (indexes instanceof Integer) {
            addBinding((Integer) indexes, codecs.encode(value, context));
        } else {
            addBinding((Query.Indexes) indexes, codecs.encode(value, context));
        }

        return this;
    }

    @Override
    public MySqlStatement bindNull(int index, Class<?> type) {
        // Useless, but should be checked in here, for programming robustness
        requireNonNull(type, "type must not be null");

        addBinding(index, codecs.encodeNull());
        return this;
    }

    @Override
    public MySqlStatement bindNull(String name, Class<?> type) {
        requireNonNull(name, "name must not be null");
        // Useless, but should be checked in here, for programming robustness
        requireNonNull(type, "type must not be null");

        Object indexes = query.getIndexes(name);

        if (indexes instanceof Integer) {
            addBinding((Integer) indexes, codecs.encodeNull());
        } else {
            addBinding((Query.Indexes) indexes, codecs.encodeNull());
        }

        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        if (bindings.bindings.isEmpty()) {
            throw new IllegalStateException("No parameters bound for current statement");
        }
        bindings.validatedFinish();

        return Flux.defer(() -> {
            if (!executed.compareAndSet(false, true)) {
                return Flux.error(new IllegalStateException("Statement was already executed"));
            }

            String sql = query.getSql();

            return QueryFlow.prepare(client, sql)
                .doOnCancel(bindings::clear)
                .flatMapMany(id -> QueryFlow.execute(client, sql, id, bindings.bindings)
                    .windowUntil(QueryFlow.RESULT_DONE)
                    .map(messages -> new MySqlResult(true, codecs, context, generatedKeyName, messages))
                    .onErrorResume(e -> QueryFlow.close(client, id).then(Mono.error(e)))
                    .concatWith(QueryFlow.close(client, id).then(Mono.empty())));
        });
    }

    private void addBinding(int index, ParameterValue value) {
        assertNotExecuted();

        this.bindings.getCurrent().add(index, value);
    }

    private void addBinding(Query.Indexes indexes, ParameterValue value) {
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

        private void clear() {
            for (Binding binding : bindings) {
                binding.clear();
            }

            bindings.clear();
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
