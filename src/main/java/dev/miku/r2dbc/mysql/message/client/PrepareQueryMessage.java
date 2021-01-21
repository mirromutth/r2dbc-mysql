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
import reactor.core.publisher.Flux;

import java.nio.charset.Charset;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A message of prepare sql query for get prepared statement ID and information.
 */
public final class PrepareQueryMessage implements ClientMessage {

    private static final byte PREPARE_FLAG = 0x16;

    private final String sql;

    public PrepareQueryMessage(String sql) {
        this.sql = sql;
    }

    @Override
    public Flux<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(context, "context must not be null");

        return Flux.defer(() -> {
            Charset charset = context.getClientCollation().getCharset();
            ByteBuf buf = allocator.buffer();

            try {
                buf.writeByte(PREPARE_FLAG).writeCharSequence(sql, charset);
                return Flux.just(buf);
            } catch (Throwable e) {
                // Maybe IndexOutOfBounds or OOM (too large sql)
                buf.release();
                return Flux.error(e);
            }
        });
    }

    @Override
    public String toString() {
        return "PrepareQueryMessage{sql=REDACTED}";
    }
}
