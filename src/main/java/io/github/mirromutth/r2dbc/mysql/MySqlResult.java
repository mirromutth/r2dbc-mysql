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
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ColumnMetadataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.FictitiousRowMetadataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.RowMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.function.BiFunction;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Result} representing the results of a query against the MySQL database.
 */
public final class MySqlResult implements Result {

    private static final Logger logger = LoggerFactory.getLogger(MySqlResult.class);

    private final String sql;

    private final Converters converters;

    private final Flux<ServerMessage> messages;

    private volatile MySqlRowMetadata rowMetadata;

    private volatile Throwable throwable;

    MySqlResult(String sql, Converters converters, Flux<ServerMessage> messages) {
        this.sql = requireNonNull(sql, "sql must not be null");
        this.converters = requireNonNull(converters, "converters must not be null");
        this.messages = requireNonNull(messages, "messages must not be null");
    }

    @Override
    public Mono<Integer> getRowsUpdated() {
        return messages().<Integer>handle((message, sink) -> {
            if (message instanceof OkMessage) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Incoming row count: {}", message);
                }
                sink.next((int) ((OkMessage) message).getAffectedRows());
            } else {
                ReferenceCountUtil.release(message);
            }
        }).reduce(Integer::sum);
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        requireNonNull(f, "mapping function must not be null");

        return messages().handle((message, sink) -> {
            if (message instanceof FictitiousRowMetadataMessage) {
                processRowMetadata((FictitiousRowMetadataMessage) message);
            } else if (message instanceof RowMessage) {
                processRow((RowMessage) message, f, sink);
            } else {
                ReferenceCountUtil.release(message);
            }
        });
    }

    private Flux<ServerMessage> messages() {
        return messages.handle((message, sink) -> {
            if (message instanceof ErrorMessage) {
                sink.error(ExceptionFactory.createException((ErrorMessage) message, this.sql));
                return;
            }

            sink.next(message);

            if (message instanceof OkMessage) {
                sink.complete();
            }
        });
    }

    private void processRowMetadata(FictitiousRowMetadataMessage message) {
        ColumnMetadataMessage[] metadataMessages = message.getMessages();

        if (metadataMessages.length == 0) {
            return;
        }

        this.rowMetadata = MySqlRowMetadata.create(metadataMessages);
    }

    private <T> void processRow(RowMessage message, BiFunction<Row, RowMetadata, ? extends T> f, SynchronousSink<T> sink) {
        MySqlRowMetadata rowMetadata = this.rowMetadata;

        if (rowMetadata == null) {
            sink.error(new IllegalStateException("No MySqlRowMetadata available"));
            return;
        }

        MySqlRow row = new MySqlRow(message.getFields(), rowMetadata, this.converters);
        T t;

        try {
            // Can NOT just sink.next(f.apply(...)) because of finally release
            t = f.apply(row, rowMetadata);
        } finally {
            // Maybe should catch and ignore exceptions in releasing?
            row.release();
        }

        sink.next(t);
    }
}
