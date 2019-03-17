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

import io.github.mirromutth.r2dbc.mysql.constant.CommandType;
import io.github.mirromutth.r2dbc.mysql.constant.ProtocolLifecycle;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessageDecoder;
import io.github.mirromutth.r2dbc.mysql.message.backend.CompleteMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.CommandMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Encode and decode state machine.
 *
 * @see BackendMessageDecoder
 * @see FrontendMessage#encode(ByteBufAllocator, AtomicInteger, MySqlSession)
 */
public final class MessageCodec {

    private final AtomicInteger sequenceId = new AtomicInteger();

    private final BackendMessageDecoder decoder = new BackendMessageDecoder();

    private final AtomicReference<ProtocolLifecycle> lifecycle = new AtomicReference<>(ProtocolLifecycle.CONNECTION);

    private final Queue<CommandType> commands = Queues.<CommandType>unbounded().get();

    public Mono<BackendMessage> decode(ByteBuf envelope, MySqlSession session) {
        BackendMessage message = decoder.decode(envelope, lifecycle.get(), sequenceId, commands.peek(), session);

        if (message == null) {
            return Mono.empty();
        }

        if (message instanceof CompleteMessage) {
            processComplete();
        }

        return Mono.just(message);
    }

    public Flux<ByteBuf> encode(FrontendMessage message, ByteBufAllocator bufAllocator, MySqlSession session) {
        requireNonNull(message, "message must not be null");

        if (this.lifecycle.get() == ProtocolLifecycle.COMMAND) {
            if (!(message instanceof CommandMessage)) {
                throw new IllegalArgumentException("message must be a command on command phase.");
            }

            if (message.isExchanged()) {
                commands.add(((CommandMessage) message).getCommandType());
            }
        }

        return message.encode(bufAllocator, sequenceId, session);
    }

    public void dispose() {
        decoder.dispose();
    }

    private void processComplete() {
        ProtocolLifecycle lifecycle = this.lifecycle.get();

        if (lifecycle == ProtocolLifecycle.CONNECTION) {
            this.lifecycle.compareAndSet(ProtocolLifecycle.CONNECTION, ProtocolLifecycle.COMMAND);
        } else if (lifecycle == ProtocolLifecycle.COMMAND) {
            this.commands.remove();
        }
    }
}
