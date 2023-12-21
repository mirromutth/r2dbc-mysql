/*
 * Copyright 2018-2021 the original author or authors.
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
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.OkMessage;
import dev.miku.r2dbc.mysql.message.server.RowMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import dev.miku.r2dbc.mysql.util.NettyBufferUtils;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.annotation.Nullable;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Result} representing the results of a query against the MySQL database.
 * <p>
 * A {@link Segment} provided by this implementation may be both {@link UpdateCount} and {@link RowSegment},
 * see also {@link MySqlOkSegment}. It's based on a {@link OkMessage}, when the {@code generatedKeyName} is
 * not {@code null}.
 */
public final class MySqlResult implements Result {

    private static final Consumer<ReferenceCounted> RELEASE = ReferenceCounted::release;

    private static final BiConsumer<Segment, SynchronousSink<Integer>> ROWS_UPDATED = (segment, sink) -> {
        if (segment instanceof UpdateCount) {
            sink.next((int) ((UpdateCount) segment).value());
        } else if (segment instanceof Message) {
            sink.error(((Message) segment).exception());
        } else if (segment instanceof ReferenceCounted) {
            ReferenceCountUtil.safeRelease(segment);
        }
    };

    private static final BiFunction<Integer, Integer, Integer> SUM = Integer::sum;

    private final Flux<Segment> segments;

    private MySqlResult(Flux<Segment> segments) {
        this.segments = segments;
    }

    @Override
    public Mono<Long> getRowsUpdated() {
        return segments.handle(ROWS_UPDATED).reduce(SUM).map(i->(long)i);
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        requireNonNull(f, "mapping function must not be null");

        return segments.handle((segment, sink) -> {
            if (segment instanceof RowSegment) {
                Row row = ((RowSegment) segment).row();

                try {
                    sink.next(f.apply(row, row.getMetadata()));
                } finally {
                    ReferenceCountUtil.safeRelease(segment);
                }
            } else if (segment instanceof Message) {
                sink.error(((Message) segment).exception());
            } else if (segment instanceof ReferenceCounted) {
                ReferenceCountUtil.safeRelease(segment);
            }
        });
    }

    @Override
    public <T> Flux<T> map(Function<? super Readable, ? extends T> f) {
        requireNonNull(f, "mapping function must not be null");

        return segments.handle((segment, sink) -> {
            if (segment instanceof RowSegment) {
                try {
                    sink.next(f.apply(((RowSegment) segment).row()));
                } finally {
                    ReferenceCountUtil.safeRelease(segment);
                }
            } else if (segment instanceof Message) {
                sink.error(((Message) segment).exception());
            } else if (segment instanceof ReferenceCounted) {
                ReferenceCountUtil.safeRelease(segment);
            }
        });
    }

    @Override
    public MySqlResult filter(Predicate<Segment> filter) {
        requireNonNull(filter, "filter must not be null");

        return new MySqlResult(segments.filter(segment -> {
            if (filter.test(segment)) {
                return true;
            }

            if (segment instanceof ReferenceCounted) {
                ReferenceCountUtil.safeRelease(segment);
            }

            return false;
        }));
    }

    @Override
    public <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> f) {
        requireNonNull(f, "mapping function must not be null");

        return segments.flatMap(segment -> {
            Publisher<? extends T> ret = f.apply(segment);

            if (ret == null) {
                return Mono.error(new IllegalStateException("The mapper returned a null Publisher"));
            }

            // doAfterTerminate to not release resources before they had a chance to get emitted.
            if (ret instanceof Mono) {
                @SuppressWarnings("unchecked")
                Mono<T> mono = (Mono<T>) ret;
                return mono.doAfterTerminate(() -> ReferenceCountUtil.release(segment));
            }

            return Flux.from(ret).doAfterTerminate(() -> ReferenceCountUtil.release(segment));
        });
    }

    static MySqlResult toResult(boolean binary, Codecs codecs, ConnectionContext context,
        @Nullable String generatedKeyName, Flux<ServerMessage> messages) {
        requireNonNull(codecs, "codecs must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(messages, "messages must not be null");

        return new MySqlResult(OperatorUtils.discardOnCancel(messages)
            .doOnDiscard(ReferenceCounted.class, RELEASE)
            .handle(new MySqlSegments(binary, codecs, context, generatedKeyName)));
    }

    private static final class MySqlMessage implements Message {

        private final ErrorMessage message;

        private MySqlMessage(ErrorMessage message) {
            this.message = message;
        }

        @Override
        public R2dbcException exception() {
            return message.toException();
        }

        @Override
        public int errorCode() {
            return message.getCode();
        }

        @Override
        public String sqlState() {
            return message.getSqlState();
        }

        @Override
        public String message() {
            return message.getMessage();
        }
    }

    private static final class MySqlRowSegment extends AbstractReferenceCounted implements RowSegment {

        private final MySqlRow row;

        private final FieldValue[] fields;

        private MySqlRowSegment(FieldValue[] fields, MySqlRowMetadata metadata, Codecs codecs, boolean binary,
            ConnectionContext context) {
            this.row = new MySqlRow(fields, metadata, codecs, binary, context);
            this.fields = fields;
        }

        @Override
        public Row row() {
            return row;
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            if (this.fields.length == 0) {
                return this;
            }

            for (FieldValue field : this.fields) {
                field.touch(hint);
            }

            return this;
        }

        @Override
        protected void deallocate() {
            NettyBufferUtils.releaseAll(fields);
        }
    }

    private static class MySqlUpdateCount implements UpdateCount {

        protected final OkMessage message;

        private MySqlUpdateCount(OkMessage message) {
            this.message = message;
        }

        @Override
        public long value() {
            return message.getAffectedRows();
        }
    }

    private static final class MySqlOkSegment extends MySqlUpdateCount implements RowSegment {

        private final Codecs codecs;

        private final String keyName;

        private MySqlOkSegment(OkMessage message, Codecs codecs, String keyName) {
            super(message);

            this.codecs = codecs;
            this.keyName = keyName;
        }

        @Override
        public Row row() {
            return new InsertSyntheticRow(codecs, keyName, message.getLastInsertId());
        }
    }

    private static final class MySqlSegments implements BiConsumer<ServerMessage, SynchronousSink<Segment>> {

        private final boolean binary;

        private final Codecs codecs;

        private final ConnectionContext context;

        @Nullable
        private final String generatedKeyName;

        private MySqlRowMetadata rowMetadata;

        private MySqlSegments(boolean binary, Codecs codecs, ConnectionContext context,
            @Nullable String generatedKeyName) {
            this.binary = binary;
            this.codecs = codecs;
            this.context = context;
            this.generatedKeyName = generatedKeyName;
        }

        @Override
        public void accept(ServerMessage message, SynchronousSink<Segment> sink) {
            if (message instanceof RowMessage) {
                MySqlRowMetadata metadata = this.rowMetadata;

                if (metadata == null) {
                    ReferenceCountUtil.safeRelease(message);
                    sink.error(new IllegalStateException("No MySqlRowMetadata available"));
                    return;
                }

                FieldValue[] fields;

                try {
                    fields = ((RowMessage) message).decode(binary, metadata.unwrap());
                } finally {
                    ReferenceCountUtil.safeRelease(message);
                }

                sink.next(new MySqlRowSegment(fields, metadata, codecs, binary, context));
            } else if (message instanceof SyntheticMetadataMessage) {
                DefinitionMetadataMessage[] metadataMessages = ((SyntheticMetadataMessage) message).unwrap();

                if (metadataMessages.length == 0) {
                    return;
                }

                this.rowMetadata = MySqlRowMetadata.create(metadataMessages);
            } else if (message instanceof OkMessage) {
                Segment segment = generatedKeyName == null ? new MySqlUpdateCount((OkMessage) message) :
                    new MySqlOkSegment((OkMessage) message, codecs, generatedKeyName);

                sink.next(segment);
            } else if (message instanceof ErrorMessage) {
                sink.next(new MySqlMessage((ErrorMessage) message));
            } else {
                ReferenceCountUtil.safeRelease(message);
            }
        }
    }
}
