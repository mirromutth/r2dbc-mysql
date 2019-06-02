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

import io.github.mirromutth.r2dbc.mysql.converter.Converters;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlResult} representing the results of a simple query against the MySQL database.
 */
final class SimpleMySqlResult extends MySqlResult {

    private static final BiFunction<Long, Long, Long> SUM = Math::addExact;

    private final Flux<ServerMessage> messages;

    /**
     * @param messages should has complete signal.
     */
    SimpleMySqlResult(Converters converters, MySqlSession session, Flux<ServerMessage> messages) {
        super(converters, session);
        this.messages = requireNonNull(messages, "messages must not be null");
    }

    @Override
    public Mono<Long> getRowsAffected() {
        return messages.<Long>handle((message, sink) -> {
            if (message instanceof OkMessage) {
                sink.next(((OkMessage) message).getAffectedRows());
                sink.complete();
            } else {
                ReferenceCountUtil.release(message);
            }
        }).reduce(SUM);
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        requireNonNull(f, "mapping function must not be null");

        return messages.handle((message, sink) -> {
            if (message instanceof OkMessage) {
                sink.complete();
            } else {
                handleNoComplete(message, f, sink);
            }
        });
    }
}
