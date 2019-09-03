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
import io.github.mirromutth.r2dbc.mysql.message.client.PrepareQueryMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.PreparedOkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.CommandDoneMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.R2dbcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * Parametrized query message flow for {@link ParametrizedMySqlStatement}.
 */
final class PrepareQueryFlow {

    private static final Predicate<ServerMessage> PREPARE_DONE = message ->
        message instanceof ErrorMessage || (message instanceof SyntheticMetadataMessage && ((SyntheticMetadataMessage) message).isCompleted());

    private static final Predicate<ServerMessage> EXECUTE_DONE = message ->
        message instanceof ErrorMessage || (message instanceof CommandDoneMessage && ((CommandDoneMessage) message).isDone());

    private static final Logger logger = LoggerFactory.getLogger(PrepareQueryFlow.class);

    private PrepareQueryFlow() {
    }

    static Mono<StatementMetadata> prepare(Client client, String sql) {
        return client.exchange(new PrepareQueryMessage(sql), PREPARE_DONE)
            .<StatementMetadata>handle((message, sink) -> {
                if (message instanceof ErrorMessage) {
                    sink.error(ExceptionFactory.createException((ErrorMessage) message, sql));
                } else if (message instanceof PreparedOkMessage) {
                    PreparedOkMessage preparedOk = (PreparedOkMessage) message;
                    sink.next(new StatementMetadata(client, sql, preparedOk.getStatementId()));
                } else {
                    ReferenceCountUtil.release(message);
                }
            })
            .last(); // Fetch last for wait on complete.
    }

    static Flux<Flux<ServerMessage>> execute(Client client, StatementMetadata metadata, Iterator<Binding> iterator) {
        EmitterProcessor<Binding> bindingEmitter = EmitterProcessor.create(true);

        return bindingEmitter.startWith(iterator.next())
            .onErrorResume(e -> metadata.close().then(Mono.error(e)))
            .map(binding -> client.exchange(binding.toMessage(metadata.getStatementId()), EXECUTE_DONE)
                .handle((response, sink) -> {
                    if (response instanceof ErrorMessage) {
                        R2dbcException e = ExceptionFactory.createException((ErrorMessage) response, metadata.getSql());
                        try {
                            sink.error(e);
                        } finally {
                            // Statement will close because of `onError`
                            bindingEmitter.onError(e);
                        }
                        return;
                    }

                    sink.next(response);

                    // Metadata EOF message will be not receive in here.
                    if (response instanceof CommandDoneMessage && ((CommandDoneMessage) response).isDone()) {
                        if (bindingEmitter.isCancelled()) {
                            metadata.close().subscribe(null, e -> logger.error("Statement {} close failed", metadata.getStatementId(), e));
                            return;
                        }

                        if (tryNextBinding(iterator, bindingEmitter)) {
                            // Completed, close metadata.
                            metadata.close().subscribe(null, e -> {
                                logger.error("Statement {} close failed", metadata.getStatementId(), e);
                                bindingEmitter.onComplete();
                            }, bindingEmitter::onComplete);
                        }
                    }
                }));
    }

    private static boolean tryNextBinding(Iterator<Binding> iterator, EmitterProcessor<Binding> boundRequests) {
        try {
            if (iterator.hasNext()) {
                boundRequests.onNext(iterator.next());
                return false;
            }
        } catch (Exception e) {
            // Statement will close because of `onError`
            boundRequests.onError(e);
            return false;
        }

        // Has completed.
        return true;
    }
}
