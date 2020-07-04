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

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.message.client.ClientMessage;
import dev.miku.r2dbc.mysql.message.client.PrepareQueryMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedCloseMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedFetchMessage;
import dev.miku.r2dbc.mysql.message.client.SimpleQueryMessage;
import dev.miku.r2dbc.mysql.message.server.CompleteMessage;
import dev.miku.r2dbc.mysql.message.server.EofMessage;
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.PreparedOkMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.ServerStatusMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import dev.miku.r2dbc.mysql.util.InternalArrays;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A message flow considers both of parametrized and text queries, such as
 * {@link TextParametrizedStatement}, {@link PrepareParametrizedStatement},
 * {@link TextSimpleStatement}, {@link PrepareSimpleStatement} and
 * {@link MySqlBatch}.
 */
final class QueryFlow {

    // Metadata EOF message will be not receive in here.
    private static final Predicate<ServerMessage> RESULT_DONE = message -> message instanceof CompleteMessage;

    private static final Predicate<ServerMessage> FETCH_DONE = message -> message instanceof ErrorMessage ||
        (message instanceof CompleteMessage && ((CompleteMessage) message).isDone());

    private static final Consumer<ReferenceCounted> RELEASE = ReferenceCounted::release;

    private static final Consumer<Object> SAFE_RELEASE = ReferenceCountUtil::safeRelease;

    /**
     * Execute multiple bindings of a prepared statement with one-by-one. Query execution terminates with
     * the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage} will emit an
     * exception and cancel subsequent bindings execution.
     * <p>
     * It will close this prepared statement.
     *
     * @param client    the {@link Client} to exchange messages with.
     * @param sql       the original statement for exception tracing.
     * @param bindings  the data of bindings.
     * @param fetchSize the size of fetching, if it less than or equal to {@literal 0} means fetch all rows.
     * @return the messages received in response to this exchange, and will be completed
     * by {@link CompleteMessage} when it is last result for each binding.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql, List<Binding> bindings, int fetchSize) {
        if (bindings.isEmpty()) {
            return Flux.empty();
        }

        EmitterProcessor<ClientMessage> requests = EmitterProcessor.create(1, false);
        PrepareHandler handler = new PrepareHandler(requests, sql, bindings.iterator(), fetchSize);

        // TODO: discardOnCancel should supports cancel hook.
        return OperatorUtils.discardOnCancel(client.exchange(requests, handler))
            .doOnDiscard(ReferenceCounted.class, RELEASE)
            .doOnCancel(handler::close)
            .doOnError(ignored -> handler.close())
            .handle(handler)
            .windowUntil(RESULT_DONE);
    }

    static Flux<Flux<ServerMessage>> execute(Client client, TextQuery query, List<Binding> bindings) {
        if (bindings.isEmpty()) {
            return Flux.empty();
        }

        EmitterProcessor<ClientMessage> requests = EmitterProcessor.create(1, false);
        TextQueryHandler handler = new TextQueryHandler(requests, query, bindings.iterator());

        // TODO: discardOnCancel should supports cancel hook.
        return OperatorUtils.discardOnCancel(client.exchange(requests, handler))
            .doOnDiscard(ReferenceCounted.class, RELEASE)
            .doOnCancel(handler::close)
            .doOnError(ignored -> handler.close())
            .handle(handler)
            .windowUntil(RESULT_DONE);
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
        return multiQuery(client, InternalArrays.asIterator(statements))
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
                    return multiQuery(client, statements.iterator()).windowUntil(RESULT_DONE);
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
            .handle((message, sink) -> {
                if (message instanceof ErrorMessage) {
                    sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                } else {
                    sink.next(message);
                }
            });
    }

    private static Flux<ServerMessage> multiQuery(Client client, Iterator<String> statements) {
        EmitterProcessor<ClientMessage> requests = EmitterProcessor.create(1, false);
        MultiQueryHandler handler = new MultiQueryHandler(requests, statements);

        // TODO: discardOnCancel should supports cancel hook.
        return OperatorUtils.discardOnCancel(client.exchange(requests, handler))
            .doOnDiscard(ReferenceCounted.class, RELEASE)
            .doOnCancel(requests::onComplete)
            .doOnError(ignored -> requests.onComplete())
            .handle(handler);
    }

    private QueryFlow() {
    }
}

abstract class BaseHandler<T> implements BiConsumer<ServerMessage, SynchronousSink<ServerMessage>>, Predicate<ServerMessage> {

    protected final EmitterProcessor<ClientMessage> requests;

    protected final Iterator<T> iterator;

    protected BaseHandler(EmitterProcessor<ClientMessage> requests, Iterator<T> iterator) {
        this.requests = requests;
        this.iterator = iterator;
    }

    @Override
    public final boolean test(ServerMessage message) {
        if (message instanceof ErrorMessage) {
            requests.onComplete();
            return true;
        }

        if (!(message instanceof CompleteMessage) || !((CompleteMessage) message).isDone()) {
            return false;
        }

        if (requests.isTerminated()) {
            close();
            return true;
        }

        try {
            if (iterator.hasNext()) {
                requests.onNext(nextMessage(iterator.next()));
                return false;
            } else {
                requests.onComplete();
                return true;
            }
        } catch (Throwable e) {
            requests.onComplete();
            throw e;
        }
    }

    abstract protected void close();

    abstract protected ClientMessage nextMessage(T element);
}

final class TextQueryHandler extends BaseHandler<Binding> implements Consumer<String> {

    private final TextQuery query;

    private String current;

    TextQueryHandler(EmitterProcessor<ClientMessage> requests, TextQuery query, Iterator<Binding> bindings) {
        super(requests, bindings);
        Binding binding = bindings.next();

        requests.onNext(binding.toTextMessage(query, this));

        this.query = query;
    }

    @Override
    public void accept(String s) {
        this.current = s;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(ExceptionFactory.createException((ErrorMessage) message, current));
        } else {
            sink.next(message);
        }
    }

    @Override
    protected ClientMessage nextMessage(Binding element) {
        return element.toTextMessage(query, this);
    }

    @Override
    protected void close() {
        requests.onComplete();
        Binding.clearSubsequent(iterator);
    }
}

final class MultiQueryHandler extends BaseHandler<String> {

    private String current;

    MultiQueryHandler(EmitterProcessor<ClientMessage> requests, Iterator<String> statements) {
        super(requests, statements);

        String current = statements.next();
        requests.onNext(new SimpleQueryMessage(current));
        this.current = current;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(ExceptionFactory.createException((ErrorMessage) message, current));
        } else {
            sink.next(message);
        }
    }

    @Override
    protected ClientMessage nextMessage(String element) {
        current = element;
        return new SimpleQueryMessage(element);
    }

    @Override
    protected void close() {
        requests.onComplete();
    }
}

final class PrepareHandler implements BiConsumer<ServerMessage, SynchronousSink<ServerMessage>>, Predicate<ServerMessage> {

    private static final int PREPARE = 0;

    private static final int EXECUTE = 1;

    private static final int FETCH = 2;

    private final EmitterProcessor<ClientMessage> requests;

    private final String sql;

    private final Iterator<Binding> bindings;

    private final int fetchSize;

    private int mode = PREPARE;

    private PreparedOkMessage preparedOk;

    private PreparedFetchMessage fetch;

    PrepareHandler(EmitterProcessor<ClientMessage> requests, String sql, Iterator<Binding> bindings, int fetchSize) {
        requests.onNext(new PrepareQueryMessage(sql));

        this.requests = requests;
        this.sql = sql;
        this.bindings = bindings;
        this.fetchSize = fetchSize;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
            return;
        }

        switch (mode) {
            case PREPARE:
                // Ignore all messages in preparing phase.
                break;
            case EXECUTE:
                sink.next(message);
                break;
            default:
                if (message instanceof ServerStatusMessage) {
                    short statuses = ((ServerStatusMessage) message).getServerStatuses();
                    if ((statuses & ServerStatuses.LAST_ROW_SENT) != 0 || (statuses & ServerStatuses.CURSOR_EXISTS) == 0) {
                        sink.next(message);
                    }
                } else {
                    sink.next(message);
                }
                break;
        }
    }

    @Override
    public boolean test(ServerMessage message) {
        if (message instanceof ErrorMessage) {
            requests.onComplete();
            return true;
        }

        switch (mode) {
            case PREPARE:
                if (message instanceof PreparedOkMessage) {
                    PreparedOkMessage ok = (PreparedOkMessage) message;
                    int statementId = ok.getStatementId();
                    int columns = ok.getTotalColumns();
                    int parameters = ok.getTotalParameters();

                    preparedOk = ok;

                    // columns + parameters <= 0, has not metadata follow in,
                    if (columns <= -parameters) {
                        doNextExecute(statementId);
                    }
                } else if (message instanceof SyntheticMetadataMessage && ((SyntheticMetadataMessage) message).isCompleted()) {
                    doNextExecute(preparedOk.getStatementId());
                } else {
                    // This message will never be used.
                    ReferenceCountUtil.safeRelease(message);
                }

                return false;
            case EXECUTE:
                if (message instanceof CompleteMessage) {
                    // Complete message means execute phase done or fetch phase done (when cursor is not opened).
                    return fetchOrExecDone(message);
                } else if (message instanceof SyntheticMetadataMessage) {
                    EofMessage eof = ((SyntheticMetadataMessage) message).getEof();
                    if (eof instanceof ServerStatusMessage) {
                        // Non null.
                        if ((((ServerStatusMessage) eof).getServerStatuses() & ServerStatuses.CURSOR_EXISTS) != 0) {
                            doNextFetch();
                            return false;
                        }
                        // Otherwise means cursor does not be opened, wait for end of row EOF message.
                        return false;
                    }
                    // EOF is deprecated (null) or using EOF without statuses.
                    // EOF is deprecated: wait for OK message.
                    // EOF without statuses: means cursor does not be opened, wait for end of row EOF message.
                    return false;
                }

                return false;
            default:
                return fetchOrExecDone(message);
        }
    }

    void close() {
        if (preparedOk != null) {
            requests.onNext(new PreparedCloseMessage(preparedOk.getStatementId()));
        }
        Binding.clearSubsequent(bindings);
        requests.onComplete();
    }

    private void doNextExecute(int statementId) {
        Binding binding = bindings.next();

        mode = EXECUTE;
        requests.onNext(binding.toExecuteMessage(statementId, fetchSize <= 0));
    }

    private void doNextFetch() {
        mode = FETCH;
        requests.onNext(this.fetch == null ? (this.fetch = new PreparedFetchMessage(preparedOk.getStatementId(), fetchSize)) : this.fetch);
    }

    private boolean fetchOrExecDone(ServerMessage message) {
        if (!(message instanceof CompleteMessage) || !((CompleteMessage) message).isDone()) {
            return false;
        }

        if (requests.isTerminated()) {
            close();
            return true;
        }

        try {
            if (message instanceof ServerStatusMessage) {
                short statuses = ((ServerStatusMessage) message).getServerStatuses();
                if ((statuses & ServerStatuses.CURSOR_EXISTS) != 0 && (statuses & ServerStatuses.LAST_ROW_SENT) == 0) {
                    doNextFetch();
                    return false;
                }
                // Otherwise is sent last row or never open cursor.
            }

            if (bindings.hasNext()) {
                doNextExecute(preparedOk.getStatementId());
                return false;
            } else {
                close();
                return true;
            }
        } catch (Throwable e) {
            requests.onComplete();
            throw e;
        }
    }
}
