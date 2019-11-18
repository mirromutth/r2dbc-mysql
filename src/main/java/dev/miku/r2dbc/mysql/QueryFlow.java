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
import dev.miku.r2dbc.mysql.client.Exchangeable;
import dev.miku.r2dbc.mysql.message.client.PrepareQueryMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedCloseMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedExecuteMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedFetchMessage;
import dev.miku.r2dbc.mysql.message.client.SimpleQueryMessage;
import dev.miku.r2dbc.mysql.message.server.CompleteMessage;
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.PreparedOkMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import dev.miku.r2dbc.mysql.util.InternalArrays;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A message flow considers both of parametrized and simple (direct) queries
 * for {@link ParametrizedMySqlStatement}, {@link MySqlBatch}
 * and {@link SimpleMySqlStatement}.
 */
final class QueryFlow {

    // Metadata EOF message will be not receive in here.
    private static final Predicate<ServerMessage> RESULT_DONE = message -> message instanceof CompleteMessage;

    private static final Predicate<ServerMessage> PREPARE_DONE = message ->
        message instanceof ErrorMessage || (message instanceof SyntheticMetadataMessage && ((SyntheticMetadataMessage) message).isCompleted());

    private static final Predicate<ServerMessage> METADATA_DONE = message ->
        message instanceof ErrorMessage || message instanceof SyntheticMetadataMessage;

    private static final Predicate<ServerMessage> FETCH_DONE = message ->
        message instanceof ErrorMessage || (message instanceof CompleteMessage && ((CompleteMessage) message).isDone());

    private static final Consumer<ReferenceCounted> RELEASE = ReferenceCounted::release;

    private static final Consumer<Object> SAFE_RELEASE = ReferenceCountUtil::safeRelease;

    private static final Consumer<Binding> CLEAR = Binding::clear;

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
     * @param sql          the original statement for exception tracing.
     * @param statementId  the statement identifier want to execute.
     * @param deprecateEof EOF has been deprecated.
     * @param fetchSize    the size of fetching, if it less than or equal to {@literal 0} means fetch all rows.
     * @param bindings     the data of bindings.
     * @return the messages received in response to this exchange, and will be completed
     * by {@link CompleteMessage} when it is last result for each binding.
     */
    static Flux<Flux<ServerMessage>> execute(
        Client client, String sql, int statementId, boolean deprecateEof, int fetchSize, List<Binding> bindings
    ) {
        if (bindings.isEmpty()) {
            return Flux.empty();
        }

        Handler handler = new Handler(sql);

        return selfEmitter(bindings, binding -> execute0(client, statementId, deprecateEof, binding, fetchSize, handler), CLEAR)
            .windowUntil(RESULT_DONE);
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
        return selfEmitter(InternalArrays.asReadOnlyList(statements), sql -> execute0(client, sql), null)
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
                    return selfEmitter(statements, sql -> execute0(client, sql), null).windowUntil(RESULT_DONE);
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
     * @param statementId  the statement identifier want to execute.
     * @param deprecateEof EOF has been deprecated.
     * @param binding      the data of binding.
     * @param fetchSize    the size of fetching, if it less than or equal to {@literal 0} means fetch all rows.
     * @param handler      error message handler.
     * @return the messages received in response to this exchange, and will be completed by {@link CompleteMessage} when it is the last.
     */
    private static Flux<ServerMessage> execute0(
        Client client, int statementId, boolean deprecateEof, Binding binding, int fetchSize, Handler handler
    ) {
        if (fetchSize > 0) {
            PreparedExecuteMessage message = binding.toMessage(statementId, false);
            // If EOF has been deprecated, it will end by OK message (same as fetch), otherwise it will end by Metadata EOF message.
            // So do not take the last response message (i.e. OK message) for execute if EOF has been deprecated.
            Exchangeable execute = new Exchangeable(message, deprecateEof ? FETCH_DONE : METADATA_DONE, !deprecateEof);
            Exchangeable fetch = new Exchangeable(new PreparedFetchMessage(statementId, fetchSize), FETCH_DONE, true);

            return OperatorUtils.discardOnCancel(client.exchange(message, execute, fetch))
                .doOnDiscard(ReferenceCounted.class, RELEASE)
                .handle(handler);
        } else {
            return OperatorUtils.discardOnCancel(client.exchange(binding.toMessage(statementId, true), FETCH_DONE))
                .doOnDiscard(ReferenceCounted.class, RELEASE)
                .handle(handler);
        }
    }

    private static <T> Flux<ServerMessage> selfEmitter(
        Collection<? extends T> sources, Function<T, Flux<ServerMessage>> convert, @Nullable Consumer<? super T> discard
    ) {
        if (sources.isEmpty()) {
            return Flux.empty();
        }

        Iterator<? extends T> iterator = sources.iterator();
        EmitterProcessor<T> processor = EmitterProcessor.create(1, true);

        try {
            Flux<ServerMessage> results = processor.concatMap(it -> {
                Flux<ServerMessage> responses = convert.apply(it);

                return responses.doOnComplete(() -> {
                    if (processor.isCancelled() || processor.isTerminated()) {
                        return;
                    }

                    try {
                        if (iterator.hasNext()) {
                            processor.onNext(iterator.next());
                        } else {
                            processor.onComplete();
                        }
                    } catch (Throwable e) {
                        processor.onError(e);
                    }
                });
            });

            if (discard == null) {
                return results;
            }

            return results.doOnCancel(() -> discardSubsequence(iterator, discard))
                .doOnError(ignored -> discardSubsequence(iterator, discard));
        } finally {
            processor.onNext(iterator.next());
        }
    }

    private static <T> void discardSubsequence(Iterator<? extends T> iterator, Consumer<? super T> discard) {
        while (iterator.hasNext()) {
            discard.accept(iterator.next());
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
