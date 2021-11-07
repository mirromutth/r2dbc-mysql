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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A plain text SQL query message, it could include multi-statements.
 */
public final class TextQueryMessage implements ClientMessage {

    static final byte QUERY_FLAG = 3;

    private final String sql;

    /**
     * Creates a {@link TextQueryMessage} without parameter.
     *
     * @param sql plain text SQL, should not contain any parameter placeholder.
     * @throws IllegalArgumentException if {@code sql} is {@code null}.
     */
    public TextQueryMessage(String sql) {
        requireNonNull(sql, "sql must not be null");

        this.sql = sql;
    }

    @Override
    public Mono<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(context, "context must not be null");

        Charset charset = context.getClientCollation().getCharset();

        return Mono.fromSupplier(() -> {
            ByteBuf buf = allocator.buffer();

            try {
                buf.writeByte(QUERY_FLAG).writeCharSequence(sql, charset);
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
}
