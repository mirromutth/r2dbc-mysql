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
import io.github.mirromutth.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.FakeRowMetadataMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.RowMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Base class for {@link Result} implementations.
 * <p>
 * This class considers {@link #getRowsUpdated()} overflows logic,
 * and specifies the {@code Publisher} type returned by each method.
 */
public abstract class MySqlResult implements Result {

    private static final Function<Long, Integer> TO_INT = Math::toIntExact;

    private final Codecs codecs;

    private final MySqlSession session;

    private volatile MySqlRowMetadata rowMetadata;

    MySqlResult(Codecs codecs, MySqlSession session) {
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.session = requireNonNull(session, "session must not be null");
    }

    @Override
    public final Mono<Integer> getRowsUpdated() {
        return getRowsAffected().map(TO_INT);
    }

    abstract public Mono<Long> getRowsAffected();

    @Override
    abstract public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f);

    <T> void handleNoComplete(ServerMessage message, SynchronousSink<T> sink, BiFunction<Row, RowMetadata, ? extends T> f) {
        if (message instanceof FakeRowMetadataMessage) {
            processRowMetadata((FakeRowMetadataMessage) message);
        } else if (message instanceof RowMessage) {
            processRow((RowMessage) message, sink, f);
        } else {
            ReferenceCountUtil.safeRelease(message);
        }
    }

    private void processRowMetadata(FakeRowMetadataMessage message) {
        DefinitionMetadataMessage[] metadataMessages = message.unwrap();

        if (metadataMessages.length == 0) {
            return;
        }

        this.rowMetadata = MySqlRowMetadata.create(metadataMessages);
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
