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
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.r2dbc.spi.R2dbcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Iterator;

/**
 * Parametrized query message flow for {@link ParametrizedMySqlStatement}.
 */
final class PrepareQueryFlow {

    private static final Logger logger = LoggerFactory.getLogger(PrepareQueryFlow.class);

    private PrepareQueryFlow() {
    }

    static Flux<Flux<ServerMessage>> execute(Client client, StatementMetadata metadata, Iterator<Binding> iterator) {
        EmitterProcessor<Binding> bindingEmitter = EmitterProcessor.create(true);

        return bindingEmitter.startWith(iterator.next())
            .onErrorResume(e -> metadata.close().then(Mono.error(e)))
            .map(binding -> client.exchange(Mono.just(binding.toMessage(metadata.getStatementId())))
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

                    if (response instanceof OkMessage) {
                        tryNextBinding(iterator, bindingEmitter, metadata).subscribe(null, e -> {
                            logger.error("Statement {} close failed", metadata.getStatementId(), e);
                            sink.complete();
                        }, sink::complete);
                    }
                }));
    }

    private static Mono<Void> tryNextBinding(Iterator<Binding> iterator, EmitterProcessor<Binding> boundRequests, StatementMetadata metadata) {
        if (boundRequests.isCancelled()) {
            return metadata.close();
        }

        try {
            if (iterator.hasNext()) {
                boundRequests.onNext(iterator.next());
                return Mono.empty();
            }
        } catch (Exception e) {
            // Statement will close because of `onError`
            boundRequests.onError(e);
            return Mono.empty();
        }

        // Has completed.
        return metadata.close().doOnTerminate(boundRequests::onComplete);
    }
}
