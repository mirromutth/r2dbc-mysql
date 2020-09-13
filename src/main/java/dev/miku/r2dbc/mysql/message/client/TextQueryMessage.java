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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.Query;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A plain text SQL query message, it could include multi-statements.
 */
public final class TextQueryMessage extends LargeClientMessage {

    static final byte QUERY_FLAG = 3;

    private final Mono<String> sql;

    private TextQueryMessage(Mono<String> sql) {
        this.sql = sql;
    }

    @Override
    protected Publisher<ByteBuf> fragments(ByteBufAllocator allocator, ConnectionContext context) {
        Charset charset = context.getClientCollation().getCharset();

        return sql.map(it -> {
            ByteBuf buf = allocator.buffer();

            try {
                buf.writeByte(QUERY_FLAG).writeCharSequence(it, charset);
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
        return "TextQueryMessage{sql=REDACTED}";
    }

    /**
     * Create a {@link TextQueryMessage} without parameter.
     *
     * @param sql plain text SQL, should not contain any parameter placeholder.
     * @return above {@link TextQueryMessage}.
     * @throws IllegalArgumentException if {@code sql} is {@code null}.
     */
    public static TextQueryMessage of(String sql) {
        requireNonNull(sql, "sql must not be null");

        return new TextQueryMessage(Mono.just(sql));
    }

    /**
     * Create a {@link TextQueryMessage} with parameters.
     *
     * @param query  the parsed {@link Query}.
     * @param values the parameter values.
     * @return above {@link TextQueryMessage}.
     * @throws IllegalArgumentException if {@code query} or {@code values} is {@code null}.
     */
    public static TextQueryMessage of(Query query, Parameter[] values) {
        requireNonNull(query, "query must not be null");
        requireNonNull(values, "values must not be null");

        return new TextQueryMessage(ParamWriter.publish(query, values));
    }
}
