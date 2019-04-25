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

package io.github.mirromutth.r2dbc.mysql.message.backend;

import io.github.mirromutth.r2dbc.mysql.constant.ProtocolPhase;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.MessageCodec;
import io.netty.buffer.ByteBuf;
import reactor.core.Disposable;
import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Backend message decoder.
 */
public interface MessageDecoder extends Disposable {

    @Nullable
    BackendMessage decode(ByteBuf envelope, AtomicInteger sequenceId, DecodeContext context, MySqlSession session);

    ProtocolPhase protocolPhase();

    static MessageDecoder create(ProtocolPhase phase) {
        switch (requireNonNull(phase, "phase must not be null")) {
            case COMMAND:
                return new CommandMessageDecoder();
            case CONNECTION:
                return new ConnectionMessageDecoder();
        }

        throw new IllegalStateException("phase " + phase + " is unsupported now");
    }
}
