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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.List;
import java.util.function.Consumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * Parametrized query with text protocol message.
 * <p>
 * Note: it use {@link SimpleQueryMessage#QUERY_FLAG} because it is text protocol query.
 */
public final class TextQueryMessage extends LargeClientMessage implements ExchangeableMessage {

    private final List<String> sqlParts;

    private final Parameter[] values;

    private final Consumer<String> sqlProceed;

    public TextQueryMessage(List<String> sqlParts, Parameter[] values, Consumer<String> sqlProceed) {
        require(sqlParts.size() - 1 == values.length, "sql parts size must not be parameters size + 1");

        this.sqlParts = sqlParts;
        this.values = values;
        this.sqlProceed = sqlProceed;
    }

    @Override
    protected Mono<ByteBuf> fragments(ByteBufAllocator allocator, ConnectionContext context) {
        try {
            Charset charset = context.getClientCollation().getCharset();
            return ParamWriter.publish(sqlParts, values).map(sql -> {
                sqlProceed.accept(sql);

                ByteBuf buf = allocator.buffer(sql.length(), Integer.MAX_VALUE);

                buf.writeByte(SimpleQueryMessage.QUERY_FLAG)
                    .writeCharSequence(sql, charset);

                return buf;
            });
        } catch (Throwable e) {
            return Mono.error(e);
        }
    }
}
