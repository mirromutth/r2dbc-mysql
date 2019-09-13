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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.internal.AssertUtils;
import dev.miku.r2dbc.mysql.internal.ConnectionContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;

/**
 * Base class considers statement logic of prepared statement and simple statement.
 */
abstract class AbstractQueryMessage extends LargeClientMessage implements ExchangeableMessage {

    private final byte flag;

    private final CharSequence sql;

    AbstractQueryMessage(byte flag, CharSequence sql) {
        this.flag = flag;
        this.sql = AssertUtils.requireNonNull(sql, "sql must not be null");
    }

    public CharSequence getSql() {
        return sql;
    }

    @Override
    protected Publisher<ByteBuf> fragments(ByteBufAllocator allocator, ConnectionContext context) {
        Charset charset = context.getCollation().getCharset();
        ByteBuf buf = allocator.buffer(Byte.BYTES + sql.length(), Integer.MAX_VALUE);

        try {
            buf.writeByte(flag).writeCharSequence(sql, charset);
            return Mono.just(buf);
        } catch (Throwable e) {
            // Maybe IndexOutOfBounds or OOM (too large sql)
            buf.release();
            return Mono.error(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractQueryMessage)) {
            return false;
        }

        AbstractQueryMessage that = (AbstractQueryMessage) o;

        if (flag != that.flag) {
            return false;
        }
        return sql.equals(that.sql);
    }

    @Override
    public int hashCode() {
        int result = flag;
        result = 31 * result + sql.hashCode();
        return result;
    }
}
