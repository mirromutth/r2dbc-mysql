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

import io.github.mirromutth.r2dbc.mysql.constant.DecodeMode;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A receiver for backend response message.
 */
final class ResponseReceiver {

    private final MonoSink<Flux<BackendMessage>> backendSink;

    private final DecodeMode decodeMode;

    ResponseReceiver(MonoSink<Flux<BackendMessage>> backendSink, DecodeMode decodeMode) {
        this.backendSink = requireNonNull(backendSink, "backendSink must not be null");
        this.decodeMode = requireNonNull(decodeMode, "decodeMode must not be null");
    }

    void success(Flux<BackendMessage> message) {
        backendSink.success(message);
    }

    DecodeMode getDecodeMode() {
        return decodeMode;
    }
}
