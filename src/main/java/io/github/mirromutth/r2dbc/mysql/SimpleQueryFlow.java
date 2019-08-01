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
import io.github.mirromutth.r2dbc.mysql.constant.Capabilities;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.client.SimpleQueryMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.EofMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.r2dbc.spi.R2dbcException;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Simple (direct) query message flow for {@link MySqlBatch} and {@link SimpleQueryMySqlStatement}.
 */
final class SimpleQueryFlow {

    // Metadata EOF message will be not receive in here.
    private static final Predicate<ServerMessage> RESULT_END =
        message -> message instanceof OkMessage || message instanceof EofMessage;

    private SimpleQueryFlow() {
    }

    /**
     * Execute multi-query with batch. Query execution terminates with a
     * {@link ErrorMessage} and send Exception to signal.
     *
     * @param client     the {@link Client} to exchange messages with.
     * @param statements bundled sql for execute.
     * @return the messages received in response to this exchange, and will be
     * completed by {@link OkMessage} for each statement.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, List<String> statements, MySqlSession session) {
        return Flux.defer(() -> {
            int size = statements.size();

            switch (size) {
                case 0:
                    return Flux.empty();
                case 1:
                    return Flux.just(execute(client, statements.get(0)));
            }

            boolean batchSupported = (session.getCapabilities() & Capabilities.MULTI_STATEMENTS) != 0;

            if (batchSupported) {
                String sql = String.join(";", statements);
                return client.exchange(new SimpleQueryMessage(sql))
                    .<ServerMessage>handle((message, sink) -> {
                        if (message instanceof ErrorMessage) {
                            sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                        } else {
                            sink.next(message);
                        }
                    })
                    .windowUntil(RESULT_END)
                    .take(size);
            } else {
                return executeOneByOne(client, statements.iterator());
            }
        });
    }

    /**
     * Execute a simple query. Query execution terminates with a {@link ErrorMessage}
     * and send Exception to signal.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, must contain only one statement.
     * @return the messages received in response to this exchange, and will be
     * completed by {@link OkMessage}.
     */
    static Flux<ServerMessage> execute(Client client, String sql) {
        return client.exchange(new SimpleQueryMessage(sql)).handle((message, sink) -> {
            if (message instanceof ErrorMessage) {
                sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                return;
            }

            sink.next(message);

            // Metadata EOF message will be not receive in here.
            if (message instanceof OkMessage || message instanceof EofMessage) {
                sink.complete();
            }
        });
    }

    private static Flux<Flux<ServerMessage>> executeOneByOne(Client client, Iterator<String> iterator) {
        EmitterProcessor<String> statements = EmitterProcessor.create(true);
        return statements.startWith(iterator.next())
            .map(sql -> client.exchange(new SimpleQueryMessage(sql))
                .handle((response, sink) -> {
                    if (response instanceof ErrorMessage) {
                        R2dbcException e = ExceptionFactory.createException((ErrorMessage) response, sql);
                        try {
                            sink.error(e);
                        } finally {
                            statements.onError(e);
                        }
                        return;
                    }

                    sink.next(response);

                    // Metadata EOF message will be not receive in here.
                    if (response instanceof OkMessage || response instanceof EofMessage) {
                        try {
                            nextSql(iterator, statements);
                        } catch (Exception e) {
                            // Exception should be throwing to emitter, because of result has completed.
                            statements.onError(e);
                        } finally {
                            sink.complete();
                        }
                    }
                }));
    }

    private static void nextSql(Iterator<String> iterator, EmitterProcessor<String> statements) {
        if (statements.isCancelled()) {
            return;
        }

        if (iterator.hasNext()) {
            statements.onNext(iterator.next());
        } else {
            statements.onComplete();
        }
    }
}
