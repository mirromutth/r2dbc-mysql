/*
 * Copyright 2018-2020 the original author or authors.
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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.message.FieldValue;
import dev.miku.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import dev.miku.r2dbc.mysql.message.server.EofMessage;
import dev.miku.r2dbc.mysql.message.server.OkMessage;
import dev.miku.r2dbc.mysql.message.server.RowMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
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
import java.util.function.Consumer;
import java.util.function.Function;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Result} representing the results of a query against the MySQL database.
 */
public final class MySqlResult implements Result {

    private static final Function<OkMessage, Integer> ROWS_UPDATED = message -> (int) message.getAffectedRows();

    private static final Consumer<ReferenceCounted> RELEASE = ReferenceCounted::release;

    private final boolean isBinary;

    private final Codecs codecs;

    private final ConnectionContext context;

    @Nullable
    private final String generatedKeyName;

    private final AtomicReference<Flux<ServerMessage>> messages;

    private final MonoProcessor<OkMessage> okProcessor = MonoProcessor.create();

    private MySqlRowMetadata rowMetadata;

    /**
     * @param isBinary rows is binary.
     * @param messages must include complete signal.
     */
    MySqlResult(boolean isBinary, Codecs codecs, ConnectionContext context, @Nullable String generatedKeyName, Flux<ServerMessage> messages) {
        this.isBinary = isBinary;
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.context = requireNonNull(context, "context must not be null");
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
        return Flux.defer(() -> {
            Flux<ServerMessage> messages = this.messages.getAndSet(null);

            if (messages == null) {
                return Flux.error(new IllegalStateException("Source has been released"));
            }

            // Result mode, no need ok message.
            this.okProcessor.onComplete();

            return OperatorUtils.discardOnCancel(messages).doOnDiscard(ReferenceCounted.class, RELEASE);
        });
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
            ReferenceCountUtil.safeRelease(message);
            sink.error(new IllegalStateException("No MySqlRowMetadata available"));
            return;
        }

        FieldValue[] fields;
        T t;

        try {
            fields = message.decode(isBinary, rowMetadata.unwrap());
        } finally {
            // Release row messages' reader.
            ReferenceCountUtil.safeRelease(message);
        }

        try {
            // Can NOT just sink.next(f.apply(...)) because of finally release
            t = f.apply(new MySqlRow(fields, rowMetadata, codecs, isBinary, context), rowMetadata);
        } finally {
            // Release decoded field values.
            for (FieldValue field : fields) {
                ReferenceCountUtil.safeRelease(field);
            }
        }

        sink.next(t);
    }
}
