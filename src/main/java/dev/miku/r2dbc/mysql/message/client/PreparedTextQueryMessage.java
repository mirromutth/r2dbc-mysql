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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.Query;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A client prepared query message based on text protocol.
 */
public final class PreparedTextQueryMessage extends AtomicReference<Parameter[]>
    implements ClientMessage, Disposable {

    private final Query query;

    /**
     * Creates a {@link PreparedTextQueryMessage} with parameters.
     *
     * @param query  the parsed {@link Query}.
     * @param values the parameter values.
     * @throws IllegalArgumentException if {@code query} or {@code values} is {@code null}.
     */
    public PreparedTextQueryMessage(Query query, Parameter[] values) {
        super(requireNonNull(values, "values must not be null"));

        this.query = requireNonNull(query, "query must not be null");
    }

    @Override
    public void dispose() {
        Parameter[] values = getAndSet(null);

        for (Parameter value : values) {
            if (value != null) {
                value.dispose();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return get() == null;
    }

    @Override
    public Mono<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(context, "context must not be null");

        Charset charset = context.getClientCollation().getCharset();
        Flux<Parameter> parameters = Flux.defer(() -> {
            Parameter[] values = getAndSet(null);

            if (values == null) {
                return Flux.error(new IllegalStateException("Parameters have been disposed"));
            }

            return Flux.fromArray(values);
        });

        return ParamWriter.publish(query, parameters).map(it -> {
            ByteBuf buf = allocator.buffer();

            try {
                buf.writeByte(TextQueryMessage.QUERY_FLAG).writeCharSequence(it, charset);
                return buf;
            } catch (Throwable e) {
                // Maybe IndexOutOfBounds or OOM (too large sql)
                buf.release();
                throw e;
            }
        });
    }

    @Override
    public String toString() {
        return "PreparedTextQueryMessage{sql=REDACTED}";
    }
}
