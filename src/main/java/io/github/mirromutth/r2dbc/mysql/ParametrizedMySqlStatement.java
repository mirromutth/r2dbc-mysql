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

package io.github.mirromutth.r2dbc.mysql;

import io.github.mirromutth.r2dbc.mysql.client.Client;
import io.github.mirromutth.r2dbc.mysql.codec.Codecs;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.PrepareQueryMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.AbstractSyntheticMetadataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.PreparedOkMessage;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.require;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Parametrized {@link MySqlStatement} with parameter markers executed against a Microsoft SQL Server database.
 * <p>
 * MySQL uses indexed parameters which are marked by {@literal ?} without naming. This implementation uses
 * {@link PrepareQuery} to implement named parameters, and different indexes can have the same name.
 */
final class ParametrizedMySqlStatement extends MySqlStatementSupport {

    private final Client client;

    private final Codecs codecs;

    private final MySqlSession session;

    private final PrepareQuery query;

    private final Bindings bindings;

    private final AtomicBoolean executed = new AtomicBoolean();

    ParametrizedMySqlStatement(Client client, Codecs codecs, MySqlSession session, PrepareQuery query) {
        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.session = requireNonNull(session, "session must not be null");
        this.query = requireNonNull(query, "sql must not be null");
        this.bindings = new Bindings(this.query.getParameters());
    }

    @Override
    public MySqlStatementSupport add() {
        assertNotExecuted();

        this.bindings.validatedFinish();
        return this;
    }

    @Override
    public MySqlStatementSupport bind(Object identifier, Object value) {
        requireNonNull(identifier, "identifier must not be null");
        require(identifier instanceof String, "identifier must be a String");
        requireNonNull(value, "value must not be null");

        addBinding(query.getIndexes((String) identifier), codecs.encode(value, session));
        return this;
    }

    @Override
    public MySqlStatementSupport bind(int index, Object value) {
        requireNonNull(value, "value must not be null");

        addBinding(index, codecs.encode(value, session));
        return this;
    }

    @Override
    public MySqlStatementSupport bindNull(Object identifier, Class<?> type) {
        requireNonNull(identifier, "identifier must not be null");
        require(identifier instanceof String, "identifier must be a String");
        // Useless, but should be checked in here, for programming robustness
        requireNonNull(type, "type must not be null");

        addBinding(query.getIndexes((String) identifier), codecs.encodeNull());
        return this;
    }

    @Override
    public MySqlStatementSupport bindNull(int index, Class<?> type) {
        // Useless, but should be checked in here, for programming robustness
        requireNonNull(type, "type must not be null");

        addBinding(index, codecs.encodeNull());
        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        return Flux.defer(() -> {
            this.bindings.validatedFinish();

            if (!this.executed.compareAndSet(false, true)) {
                throw new IllegalStateException("Statement was already executed");
            }

            Iterator<Binding> iterator = this.bindings.iterator();

            if (!iterator.hasNext()) {
                return Flux.error(new IllegalStateException("No parameters bound for prepared statement"));
            }

            String sql = this.query.getSql();

            return this.client.exchange(Mono.just(new PrepareQueryMessage(sql)))
                .<StatementMetadata>handle((message, sink) -> {
                    if (message instanceof ErrorMessage) {
                        sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                    } else if (message instanceof AbstractSyntheticMetadataMessage) {
                        if (((AbstractSyntheticMetadataMessage) message).isCompleted()) {
                            sink.complete();
                        }
                    } else if (message instanceof PreparedOkMessage) {
                        PreparedOkMessage preparedOk = (PreparedOkMessage) message;
                        sink.next(new StatementMetadata(this.client, sql, preparedOk.getStatementId()));
                    } else {
                        ReferenceCountUtil.release(message);
                    }
                })
                .last()
                .flatMapMany(metadata -> PrepareQueryFlow.execute(client, metadata, iterator)
                    .map(messages -> new MySqlResult(codecs, session, generatedKeyName, messages)))
                .doOnCancel(bindings::clear)
                .doOnError(e -> bindings.clear());
        });
    }

    private void addBinding(int index, ParameterValue value) {
        assertNotExecuted();

        this.bindings.getCurrent().add(index, value);
    }

    private void addBinding(int[] indexes, ParameterValue value) {
        assertNotExecuted();

        Binding current = this.bindings.getCurrent();
        for (int index : indexes) {
            current.add(index, value);
        }
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
