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

package io.github.mirromutth.r2dbc.mysql.client;

import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.DecodeContext;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A context for request holder and response decode context.
 */
final class MessageContext {

    private volatile FrontendMessage request;

    private volatile DecodeContext decodeContext;

    private final MonoSink<Flux<BackendMessage>> receiver;

    MessageContext(FrontendMessage request, MonoSink<Flux<BackendMessage>> receiver) {
        this.request = requireNonNull(request, "request must not be null");
        this.receiver = requireNonNull(receiver, "receiver must not be null");
    }

    void initDecodeContext() {
        FrontendMessage request = this.request;

        if (request == null) {
            throw new IllegalStateException("A request is send once only");
        }

        this.request = null;
        this.decodeContext = request.decodeContext();
    }

    void success(Flux<BackendMessage> responses) {
        receiver.success(responses);
    }

    void error(Throwable cause) {
        receiver.error(cause);
    }

    DecodeContext getDecodeContext() {
        return decodeContext;
    }
}
