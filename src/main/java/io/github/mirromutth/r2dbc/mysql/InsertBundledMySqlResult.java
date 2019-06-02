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
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlResult} representing the results of {@code INSERT} with
 * {@code SELECT LAST_INSERT_ID()} query against the MySQL database.
 */
final class InsertBundledMySqlResult extends MySqlResult {

    private final MonoProcessor<Long> rowsUpdated = MonoProcessor.create();

    private final EmitterProcessor<ServerMessage> lastInsertId = EmitterProcessor.create();

    private final AtomicBoolean subscribed = new AtomicBoolean();

    private final Flux<ServerMessage> messages;

    InsertBundledMySqlResult(Converters converters, MySqlSession session, Flux<ServerMessage> messages) {
        super(converters, session);
        this.messages = requireNonNull(messages, "messages must not be null");
    }

    @Override
    public Mono<Long> getRowsAffected() {
        return rowsUpdated.doOnSubscribe(s -> onSubscribe());
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        return lastInsertId.doOnSubscribe(s -> onSubscribe())
            .handle((message, sink) -> handleNoComplete(message, f, sink));
    }

    private void onSubscribe() {
        if (!this.subscribed.compareAndSet(false, true)) {
            return;
        }

        this.messages.handle((message, sink) -> {
            if (rowsUpdated.isTerminated()) {
                lastInsertId.onNext(message);

                if (message instanceof OkMessage) {
                    lastInsertId.onComplete();
                }
            } else {
                if (message instanceof OkMessage) {
                    rowsUpdated.onNext(((OkMessage) message).getAffectedRows());
                } else {
                    ReferenceCountUtil.release(message);
                }
            }
        }).subscribe(null, e -> {
            if (!this.rowsUpdated.isTerminated()) {
                this.rowsUpdated.onError(e);
                this.lastInsertId.onError(e);
            } else if (!this.lastInsertId.isTerminated()) {
                this.lastInsertId.onError(e);
            }
        });
    }
}
