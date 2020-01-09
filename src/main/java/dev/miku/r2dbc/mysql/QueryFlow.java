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
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.message.client.ExchangeableMessage;
import dev.miku.r2dbc.mysql.message.client.PrepareQueryMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedCloseMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedFetchMessage;
import dev.miku.r2dbc.mysql.message.client.SimpleQueryMessage;
import dev.miku.r2dbc.mysql.message.server.CompleteMessage;
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.PreparedOkMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.ServerStatusMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import dev.miku.r2dbc.mysql.util.InternalArrays;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A message flow considers both of parametrized and simple (direct) queries
 * for {@link PrepareParametrizedStatement}, {@link MySqlBatch}
 * and {@link SimpleStatementSupport}.
 */
final class QueryFlow {

    // Metadata EOF message will be not receive in here.
    private static final Predicate<ServerMessage> RESULT_DONE = message -> message instanceof CompleteMessage;

    private static final Predicate<ServerMessage> PREPARE_DONE = message -> message instanceof ErrorMessage ||
        (message instanceof SyntheticMetadataMessage && ((SyntheticMetadataMessage) message).isCompleted());

    private static final Predicate<ServerMessage> METADATA_DONE = message -> message instanceof ErrorMessage ||
        message instanceof SyntheticMetadataMessage || (message instanceof CompleteMessage && ((CompleteMessage) message).isDone());

    private static final Predicate<ServerMessage> FETCH_DONE = message -> message instanceof ErrorMessage ||
        (message instanceof CompleteMessage && ((CompleteMessage) message).isDone());

    private static final Consumer<ReferenceCounted> RELEASE = ReferenceCounted::release;

    private static final Consumer<Object> SAFE_RELEASE = ReferenceCountUtil::safeRelease;

    /**
     * Prepare a query to an identifier of prepared statement.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the parametrize query.
     * @return prepared statement identifier.
     */
    static Mono<PreparedIdentifier> prepare(Client client, String sql) {
        return OperatorUtils.discardOnCancel(client.exchange(new PrepareQueryMessage(sql), PREPARE_DONE))
            .doOnDiscard(PreparedOkMessage.class, prepared -> close(client, prepared.getStatementId()).subscribe())
            .<PreparedIdentifier>handle((message, sink) -> {
                if (message instanceof ErrorMessage) {
                    sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                } else if (message instanceof SyntheticMetadataMessage) {
                    if (((SyntheticMetadataMessage) message).isCompleted()) {
                        sink.complete(); // Must wait for last metadata message.
                    }
                } else if (message instanceof PreparedOkMessage) {
                    sink.next(new PreparedIdentifier(client, ((PreparedOkMessage) message).getStatementId()));
                } else {
                    ReferenceCountUtil.release(message);
                }
            })
            .last(); // Fetch last for wait on complete, and `last` will emit exception signal when Flux is empty.
    }

    /**
     * Execute multiple bindings of a prepared statement with one-by-one. Query execution terminates with
     * the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage} will emit an
     * exception and cancel subsequent bindings execution.
     * <p>
     * It will not close this prepared statement.
     *
     * @param client       the {@link Client} to exchange messages with.
     * @param context      the connection context.
     * @param sql          the original statement for exception tracing.
     * @param identifier   the statement identifier want to execute.
     * @param deprecateEof EOF has been deprecated.
     * @param fetchSize    the size of fetching, if it less than or equal to {@literal 0} means fetch all rows.
     * @param bindings     the data of bindings.
     * @return the messages received in response to this exchange, and will be completed
     * by {@link CompleteMessage} when it is last result for each binding.
     */
    static Flux<Flux<ServerMessage>> execute(
        Client client, ConnectionContext context, String sql, PreparedIdentifier identifier, boolean deprecateEof, int fetchSize, List<Binding> bindings
    ) {
        switch (bindings.size()) {
            case 1: // Most case.
                return Flux.defer(() -> execute0(client, context, sql, identifier, deprecateEof, bindings.get(0), fetchSize)
                    .windowUntil(RESULT_DONE));
            case 0:
                return Flux.empty();
            default:
                Iterator<Binding> iterator = bindings.iterator();
                EmitterProcessor<Binding> processor = EmitterProcessor.create(1, true);

                processor.onNext(iterator.next());

                // One binding may be leak when a emitted Result has been ignored, but Result should never be ignored.
                // Subsequent bindings will auto-clear if subscribing has been cancelled.
                return processor.concatMap(it -> execute0(client, context, sql, identifier, deprecateEof, it, fetchSize)
                    .doOnComplete(() -> {
                        if (processor.isCancelled() || processor.isTerminated()) {
                            return;
                        }

                        try {
                            if (iterator.hasNext()) {
                                if (identifier.isClosed()) {
                                    // User cancelled fetching, discard subsequent bindings.
                                    discardBindings(iterator);
                                    processor.onComplete();
                                } else {
                                    processor.onNext(iterator.next());
                                }
                            } else {
                                processor.onComplete();
                            }
                        } catch (Throwable e) {
                            processor.onError(e);
                        }
                    }))
                    .doOnCancel(() -> discardBindings(iterator))
                    .doOnError(ignored -> discardBindings(iterator))
                    .windowUntil(RESULT_DONE);
        }
    }

    /**
     * Close a prepared statement and release its resources in database server.
     *
     * @param client      the {@link Client} to exchange messages with.
     * @param statementId the statement identifier want to close.
     * @return close sent signal.
     */
    static Mono<Void> close(Client client, int statementId) {
        return client.sendOnly(new PreparedCloseMessage(statementId));
    }

    /**
     * Execute a simple query and return a {@link Mono} for the complete signal or error. Query execution terminates with
     * the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange, and will be
     * completed by {@link CompleteMessage} for each statement.
     */
    static Mono<Void> executeVoid(Client client, String sql) {
        return Mono.defer(() -> execute0(client, sql).doOnNext(SAFE_RELEASE).then());
    }

    /**
     * Execute multiple simple queries with one-by-one and return a {@link Mono} for the complete signal or
     * error. Query execution terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}.
     * The {@link ErrorMessage} will emit an exception and cancel subsequent statements execution.
     *
     * @param client     the {@link Client} to exchange messages with.
     * @param statements the queries to execute, each element can be contains multi-statements.
     * @return the messages received in response to this exchange, and will be
     * completed by {@link CompleteMessage} for each statement.
     */
    static Mono<Void> executeVoid(Client client, String... statements) {
        return selfEmitter(InternalArrays.asReadOnlyList(statements), client)
            .doOnNext(SAFE_RELEASE)
            .then();
    }

    /**
     * Execute a simple compound query and return its results. Query execution terminates with
     * each {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage} will
     * emit an exception.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange, and will be completed by {@link CompleteMessage} when it is the last.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql) {
        return Flux.defer(() -> execute0(client, sql).windowUntil(RESULT_DONE));
    }

    /**
     * Execute multiple simple compound queries with one-by-one and return their results. Query execution
     * terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage}
     * will emit an exception and cancel subsequent statements execution.
     *
     * @param client     the {@link Client} to exchange messages with.
     * @param statements bundled sql for execute.
     * @return the messages received in response to this exchange, and will be
     * completed by {@link CompleteMessage} for each statement.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, List<String> statements) {
        return Flux.defer(() -> {
            switch (statements.size()) {
                case 0:
                    return Flux.empty();
                case 1:
                    return execute0(client, statements.get(0)).windowUntil(RESULT_DONE);
                default:
                    return selfEmitter(statements, client).windowUntil(RESULT_DONE);
            }
        });
    }

    /**
     * Execute a simple query. Query execution terminates with the last {@link CompleteMessage} or
     * a {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange, and will be completed by {@link CompleteMessage} when it is the last.
     */
    private static Flux<ServerMessage> execute0(Client client, String sql) {
        return OperatorUtils.discardOnCancel(client.exchange(new SimpleQueryMessage(sql), FETCH_DONE))
            .doOnDiscard(ReferenceCounted.class, RELEASE)
            .handle(new Handler(sql));
    }

    /**
     * Execute a prepared query. Query execution terminates with the last {@link CompleteMessage} or
     * a {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception.
     *
     * @param client       the {@link Client} to exchange messages with.
     * @param context      the connection context, for cursor status.
     * @param sql          the original statement for exception tracing.
     * @param identifier   the statement identifier want to execute.
     * @param deprecateEof EOF has been deprecated.
     * @param binding      the data of binding.
     * @param fetchSize    the size of fetching, if it less than or equal to {@literal 0} means fetch all rows.
     * @return the messages received in response to this exchange, and will be completed by {@link CompleteMessage} when it is the last.
     */
    private static Flux<ServerMessage> execute0(
        Client client, ConnectionContext context, String sql, PreparedIdentifier identifier, boolean deprecateEof, Binding binding, int fetchSize
    ) {
        if (fetchSize > 0) {
            int statementId = identifier.getId();
            ExchangeableMessage cursor = binding.toMessage(statementId, false);
            // If EOF has been deprecated, it will end by OK message (same as fetch), otherwise it will end by Metadata EOF message.
            // So do not take the last response message (i.e. OK message) for execute if EOF has been deprecated.
            return OperatorUtils.discardOnCancel(client.exchange(cursor, deprecateEof ? FETCH_DONE : METADATA_DONE))
                .doOnDiscard(ReferenceCounted.class, RELEASE)
                .handle(new TakeOne(sql)) // Should wait to complete, then concat fetches.
                .concatWith(Flux.defer(() -> fetch(client, context, identifier, new PreparedFetchMessage(statementId, fetchSize), sql)));
        } else {
            return OperatorUtils.discardOnCancel(client.exchange(binding.toMessage(identifier.getId(), true), FETCH_DONE))
                .doOnDiscard(ReferenceCounted.class, RELEASE)
                .handle(new Handler(sql));
        }
    }

    private static Flux<ServerMessage> fetch(
        Client client, ConnectionContext context, PreparedIdentifier identifier, PreparedFetchMessage fetch, String sql
    ) {
        if ((context.getServerStatuses() & ServerStatuses.CURSOR_EXISTS) == 0) {
            // No opened cursor, means current statement is update query.
            return Flux.empty();
        }

        EmitterProcessor<ExchangeableMessage> processor = EmitterProcessor.create(1, false);

        processor.onNext(fetch);

        return processor.concatMap(request -> OperatorUtils.discardOnCancel(client.exchange(request, FETCH_DONE))
            .doOnDiscard(ReferenceCounted.class, RELEASE)
            .handle((message, sink) -> {
                if (message instanceof ErrorMessage) {
                    sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                    processor.onComplete();
                } else if (message instanceof ServerStatusMessage) {
                    // It is also can be read from Connection Context.
                    short statuses = ((ServerStatusMessage) message).getServerStatuses();

                    if ((statuses & ServerStatuses.LAST_ROW_SENT) == 0) {
                        // This exchange does not contains last row, so drop the complete frame of this exchange.
                        if (processor.isCancelled() || processor.isTerminated()) {
                            return;
                        }

                        if (identifier.isClosed()) {
                            // User cancelled fetching, no need do anything.
                            processor.onComplete();
                        } else {
                            processor.onNext(fetch);
                        }
                    } else {
                        // It is complete frame and this exchange contains last row.
                        sink.next(message);
                        processor.onComplete();
                    }
                } else {
                    sink.next(message);
                }
            }));
    }

    private static Flux<ServerMessage> selfEmitter(Collection<String> statements, Client client) {
        if (statements.isEmpty()) {
            return Flux.empty();
        }

        Iterator<String> iter = statements.iterator();
        EmitterProcessor<String> processor = EmitterProcessor.create(1, true);

        processor.onNext(iter.next());

        return processor.concatMap(it -> {
            Flux<ServerMessage> responses = execute0(client, it);

            return responses.doOnComplete(() -> {
                if (processor.isCancelled() || processor.isTerminated()) {
                    return;
                }

                try {
                    if (iter.hasNext()) {
                        processor.onNext(iter.next());
                    } else {
                        processor.onComplete();
                    }
                } catch (Throwable e) {
                    processor.onError(e);
                }
            });
        });
    }

    private static void discardBindings(Iterator<Binding> iterator) {
        while (iterator.hasNext()) {
            iterator.next().clear();
        }
    }

    private static final class TakeOne implements BiConsumer<ServerMessage, SynchronousSink<ServerMessage>> {

        private final String sql;

        private boolean next;

        private TakeOne(String sql) {
            this.sql = sql;
        }

        @Override
        public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
            if (next) {
                return;
            }
            next = true;

            if (message instanceof ErrorMessage) {
                sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
            } else {
                sink.next(message);
            }
        }
    }

    private static final class Handler implements BiConsumer<ServerMessage, SynchronousSink<ServerMessage>> {

        private final String sql;

        private Handler(String sql) {
            this.sql = sql;
        }

        @Override
        public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
            if (message instanceof ErrorMessage) {
                sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
            } else {
                sink.next(message);
            }
        }
    }

    private QueryFlow() {
    }
}
