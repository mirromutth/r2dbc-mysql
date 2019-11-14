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
import dev.miku.r2dbc.mysql.message.client.PrepareQueryMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedCloseMessage;
import dev.miku.r2dbc.mysql.message.client.SimpleQueryMessage;
import dev.miku.r2dbc.mysql.message.server.CompleteMessage;
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.PreparedOkMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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

    private static final Predicate<ServerMessage> EXECUTE_DONE = message ->
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
    static Mono<Integer> prepare(Client client, String sql) {
        return OperatorUtils.discardOnCancel(client.exchange(new PrepareQueryMessage(sql), PREPARE_DONE))
            .doOnDiscard(PreparedOkMessage.class, prepared -> close(client, prepared.getStatementId()).subscribe())
            .<Integer>handle((message, sink) -> {
                if (message instanceof ErrorMessage) {
                    sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                } else if (message instanceof SyntheticMetadataMessage) {
                    if (((SyntheticMetadataMessage) message).isCompleted()) {
                        sink.complete(); // Must wait for last metadata message.
                    }
                } else if (message instanceof PreparedOkMessage) {
                    sink.next(((PreparedOkMessage) message).getStatementId());
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
     * @param client      the {@link Client} to exchange messages with.
     * @param sql         the original statement for exception tracing.
     * @param statementId the statement identifier want to execute.
     * @param bindings    the data of bindings.
     * @return the messages received in response to this exchange, and will be completed
     * by {@link CompleteMessage} when it is last result for each binding.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql, int statementId, List<Binding> bindings) {
        if (bindings.isEmpty()) {
            return Flux.empty();
        }

        Handler handler = new Handler(sql);

        return OperatorUtils.discardOnCancel(Flux.fromIterable(bindings))
            .doOnDiscard(Binding.class, CLEAR)
            .concatMap(binding -> OperatorUtils.discardOnCancel(client.exchange(binding.toMessage(statementId), EXECUTE_DONE))
                .doOnDiscard(ReferenceCounted.class, RELEASE)
                .handle(handler))
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
        return Flux.fromArray(statements).concatMap(sql -> execute0(client, sql)).doOnNext(SAFE_RELEASE).then();
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
                    return Flux.fromIterable(statements).concatMap(sql -> execute0(client, sql)).windowUntil(RESULT_DONE);
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
        return OperatorUtils.discardOnCancel(client.exchange(new SimpleQueryMessage(sql), EXECUTE_DONE))
            .doOnDiscard(ReferenceCounted.class, RELEASE)
            .handle(new Handler(sql));
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
