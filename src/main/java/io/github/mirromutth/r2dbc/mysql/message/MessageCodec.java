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

package io.github.mirromutth.r2dbc.mysql.message;

import io.github.mirromutth.r2dbc.mysql.constant.ProtocolPhase;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.DecodeContext;
import io.github.mirromutth.r2dbc.mysql.message.backend.MessageDecoder;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Encode and decode messages with codec contexts.
 *
 * @see MessageDecoder
 * @see FrontendMessage#encode(ByteBufAllocator, AtomicInteger, MySqlSession)
 */
public final class MessageCodec implements Disposable {

    private final AtomicInteger sequenceId = new AtomicInteger();

    private final AtomicReference<MessageDecoder> decoder = new AtomicReference<>(MessageDecoder.create(ProtocolPhase.CONNECTION));

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public Mono<BackendMessage> decode(ByteBuf envelope, DecodeContext context, MySqlSession session) {
        requireNonNull(envelope, "envelope must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(session, "session must not be null");

        MessageDecoder decoder = this.decoder.get();

        if (decoder == null) {
            throw new IllegalStateException("MessageCodec is disposed");
        }

        BackendMessage message = decoder.decode(envelope, sequenceId, context, session);

        if (message == null) {
            return Mono.empty();
        }

        return Mono.just(message);
    }

    public Flux<ByteBuf> encode(FrontendMessage message, ByteBufAllocator bufAllocator, MySqlSession session) {
        return requireNonNull(message, "message must not be null").encode(bufAllocator, sequenceId, session);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void changePhase(ProtocolPhase phase) {
        requireNonNull(phase, "phase must not be null");
        MessageDecoder newDecoder = null;

        while (true) {
            MessageDecoder decoder = this.decoder.get();

            if (decoder.protocolPhase() == phase) {
                if (newDecoder != null) {
                    newDecoder.dispose();
                }
                return;
            }

            if (newDecoder == null) {
                newDecoder = MessageDecoder.create(phase);
            }

            if (this.decoder.compareAndSet(decoder, newDecoder)) {
                return;
            }
        }
    }

    @Override
    public void dispose() {
        MessageDecoder decoder = this.decoder.getAndSet(null);

        if (decoder != null) {
            decoder.dispose();
        }
    }

    @Override
    public boolean isDisposed() {
        MessageDecoder decoder = this.decoder.get();

        if (decoder == null) {
            return true;
        } else {
            return decoder.isDisposed();
        }
    }
}
