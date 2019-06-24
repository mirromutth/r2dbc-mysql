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
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Iterator;

/**
 * Parametrized query message flow for {@link ParametrizedMySqlStatement}.
 */
final class PrepareQueryFlow {

    private PrepareQueryFlow() {
    }

    static Flux<Flux<ServerMessage>> execute(Client client, String sql, int statementId, Iterator<Binding> iterator) {
        EmitterProcessor<Binding> bindingEmitter = EmitterProcessor.create(true);

        return bindingEmitter.startWith(iterator.next())
            .map(binding -> client.exchange(Mono.just(binding.toMessage(statementId)))
                .handle((response, sink) -> {
                    if (response instanceof ErrorMessage) {
                        sink.error(ExceptionFactory.createException((ErrorMessage) response, sql));
                        return;
                    }

                    sink.next(response);

                    if (response instanceof OkMessage) {
                        tryNextBinding(iterator, bindingEmitter);
                        sink.complete();
                    }
                }));
    }

    private static void tryNextBinding(Iterator<Binding> iterator, EmitterProcessor<Binding> boundRequests) {
        if (boundRequests.isCancelled()) {
            return;
        }

        try {
            if (iterator.hasNext()) {
                boundRequests.onNext(iterator.next());
            } else {
                boundRequests.onComplete();
            }
        } catch (Exception e) {
            boundRequests.onError(e);
        }
    }
}
