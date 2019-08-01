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

import io.github.mirromutth.r2dbc.mysql.codec.Codecs;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.server.EofMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.RowMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Result} representing the results of a query against the MySQL database.
 */
public final class MySqlResult implements Result {

    private static final Function<OkMessage, Integer> ROWS_UPDATED = message -> (int) message.getAffectedRows();

    private final Codecs codecs;

    private final MySqlSession session;

    @Nullable
    private final String generatedKeyName;

    private final AtomicReference<Flux<ServerMessage>> messages;

    private final MonoProcessor<OkMessage> okProcessor = MonoProcessor.create();

    private volatile MySqlRowMetadata rowMetadata;

    /**
     * @param messages must include complete signal.
     */
    MySqlResult(Codecs codecs, MySqlSession session, @Nullable String generatedKeyName, Flux<ServerMessage> messages) {
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.session = requireNonNull(session, "session must not be null");
        this.generatedKeyName = generatedKeyName;
        this.messages = new AtomicReference<>(requireNonNull(messages, "messages must not be null"));
    }

    @Override
    public Mono<Integer> getRowsUpdated() {
        return affects().map(ROWS_UPDATED);
    }

    @Override
    public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        requireNonNull(f, "mapping function must not be null");

        if (generatedKeyName == null) {
            return results().handle((message, sink) -> handleResult(message, sink, f));
        } else {
            return affects().map(message -> {
                InsertSyntheticRow row = new InsertSyntheticRow(codecs, generatedKeyName, message.getLastInsertId());
                return f.apply(row, row);
            });
        }
    }

    private Mono<OkMessage> affects() {
        return this.okProcessor.doOnSubscribe(s -> {
            Flux<ServerMessage> messages = this.messages.getAndSet(null);

            if (messages == null) {
                // Has subscribed, `okProcessor` will be set or cancel.
                return;
            }

            messages.subscribe(message -> {
                if (message instanceof OkMessage) {
                    // No need check terminal because of OkMessage no need release.
                    this.okProcessor.onNext(((OkMessage) message));
                } else if (message instanceof EofMessage) {
                    // Metadata EOF message will be not receive in here.
                    // EOF message, means it is SELECT statement.
                    this.okProcessor.onComplete();
                } else {
                    ReferenceCountUtil.safeRelease(message);
                }
            }, this.okProcessor::onError, this.okProcessor::onComplete);
        });
    }

    private Flux<ServerMessage> results() {
        Flux<ServerMessage> messages = this.messages.getAndSet(null);

        if (messages == null) {
            return Flux.error(new IllegalStateException("Source has been released"));
        }

        // Result mode, no need ok message.
        this.okProcessor.onComplete();

        return messages;
    }

    private <T> void handleResult(ServerMessage message, SynchronousSink<T> sink, BiFunction<Row, RowMetadata, ? extends T> f) {
        if (message instanceof SyntheticMetadataMessage) {
            DefinitionMetadataMessage[] metadataMessages = ((SyntheticMetadataMessage) message).unwrap();
            if (metadataMessages.length == 0) {
                return;
            }
            this.rowMetadata = MySqlRowMetadata.create(metadataMessages);
        } else if (message instanceof RowMessage) {
            processRow((RowMessage) message, sink, f);
        } else {
            ReferenceCountUtil.safeRelease(message);
        }
    }

    private <T> void processRow(RowMessage message, SynchronousSink<T> sink, BiFunction<Row, RowMetadata, ? extends T> f) {
        MySqlRowMetadata rowMetadata = this.rowMetadata;

        if (rowMetadata == null) {
            sink.error(new IllegalStateException("No MySqlRowMetadata available"));
            return;
        }

        MySqlRow row = new MySqlRow(message.getFields(), rowMetadata, this.codecs, message.isBinary(), this.session);
        T t;

        try {
            // Can NOT just sink.next(f.apply(...)) because of finally release
            t = f.apply(row, rowMetadata);
        } finally {
            ReferenceCountUtil.safeRelease(row);
        }

        sink.next(t);
    }
}
