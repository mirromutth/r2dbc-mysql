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
import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.FakeRowMetadataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.RowMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

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
                FakeRow row = new FakeRow(codecs, generatedKeyName, message.getLastInsertId());
                return f.apply(row, row);
            });
        }
    }

    private Mono<OkMessage> affects() {
        return this.okProcessor.doOnSubscribe(s -> {
            Flux<ServerMessage> messages = this.messages.getAndSet(null);

            if (messages == null) {
                // Has subscribed, ok will be set or cancel.
                return;
            }

            messages.subscribe(message -> {
                if (message instanceof OkMessage) {
                    // No need check terminal because of OkMessage no need release.
                    this.okProcessor.onNext(((OkMessage) message));
                } else {
                    ReferenceCountUtil.safeRelease(message);
                }
            }, this.okProcessor::onError);
        });
    }

    private Flux<ServerMessage> results() {
        Flux<ServerMessage> messages = this.messages.getAndSet(null);

        if (messages == null) {
            throw new IllegalStateException("Source has been released");
        }

        // Result mode, no need ok message.
        okProcessor.cancel();

        return messages;
    }

    private <T> void handleResult(ServerMessage message, SynchronousSink<T> sink, BiFunction<Row, RowMetadata, ? extends T> f) {
        if (message instanceof FakeRowMetadataMessage) {
            DefinitionMetadataMessage[] metadataMessages = ((FakeRowMetadataMessage) message).unwrap();
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

    private static class FakeRow implements Row, RowMetadata, ColumnMetadata {

        private final Codecs codecs;

        private final String generatedKeyName;

        private final long lastInsertId;

        private FakeRow(Codecs codecs, String generatedKeyName, long lastInsertId) {
            this.codecs = codecs;
            this.generatedKeyName = generatedKeyName;
            this.lastInsertId = lastInsertId;
        }

        @Override
        public <T> T get(Object identifier, Class<T> type) {
            requireNonNull(type, "type must not be null");
            assertValidIdentifier(identifier);

            return get0(type);
        }

        @Override
        public Object get(Object identifier) {
            assertValidIdentifier(identifier);

            if (lastInsertId < 0) {
                // BIGINT UNSIGNED
                return get0(BigInteger.class);
            } else {
                return get0(Long.TYPE);
            }
        }

        @Override
        public ColumnMetadata getColumnMetadata(Object identifier) {
            assertValidIdentifier(identifier);

            return this;
        }

        @Override
        public Collection<ColumnMetadata> getColumnMetadatas() {
            return Collections.singletonList(this);
        }

        @Override
        public Collection<String> getColumnNames() {
            return Collections.singleton(generatedKeyName);
        }

        @Override
        public Class<?> getJavaType() {
            if (lastInsertId < 0) {
                return BigInteger.class;
            } else {
                return Long.TYPE;
            }
        }

        @Override
        public String getName() {
            return generatedKeyName;
        }

        @Override
        public Integer getNativeTypeMetadata() {
            return DataType.BIGINT.getType();
        }

        @Override
        public Nullability getNullability() {
            return Nullability.NON_NULL;
        }

        @Override
        public Integer getPrecision() {
            // The default precision of BIGINT is 20
            return 20;
        }

        @Override
        public Integer getScale() {
            // BIGINT not support scale.
            return null;
        }

        private <T> T get0(Class<T> type) {
            return codecs.decodeLastInsertId(lastInsertId, type);
        }

        private void assertValidIdentifier(Object identifier) {
            requireNonNull(identifier, "identifier must not be null");

            if (identifier instanceof Integer) {
                if ((Integer) identifier != 0) {
                    throw new ArrayIndexOutOfBoundsException((Integer) identifier);
                }
            } else if (identifier instanceof String) {
                if (!this.generatedKeyName.equals(identifier)) {
                    throw new NoSuchElementException(String.format("Column name '%s' does not exist in column names [%s]", identifier, this.generatedKeyName));
                }
            } else {
                throw new IllegalArgumentException("identifier should either be an Integer index or a String column name.");
            }
        }
    }
}
