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

package io.github.mirromutth.r2dbc.mysql.message.client;

import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A plain text SQL query message without any parameter, it could include multi-statements.
 */
public final class SimpleQueryMessage extends AbstractClientMessage implements ExchangeableMessage {

    private static final byte QUERY_FLAG = 3;

    private final String sql;

    public SimpleQueryMessage(String sql) {
        this.sql = requireNonNull(sql, "sql must not be null");
    }

    public String getSql() {
        return sql;
    }

    @Override
    public boolean isSequenceIdReset() {
        return true;
    }

    @Override
    protected ByteBuf encodeSingle(ByteBufAllocator bufAllocator, MySqlSession session) {
        final ByteBuf result = bufAllocator.buffer();

        try {
            result.writeByte(QUERY_FLAG);
            result.writeCharSequence(sql, session.getCollation().getCharset());
            return result;
        } catch (Throwable e) {
            result.release();
            throw e;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SimpleQueryMessage)) {
            return false;
        }

        SimpleQueryMessage that = (SimpleQueryMessage) o;

        return sql.equals(that.sql);
    }

    @Override
    public int hashCode() {
        return sql.hashCode();
    }

    @Override
    public String toString() {
        // SQL should NOT be printed as this may contain security information.
        // Of course, if user use trace level logs, SQL is still be printed by ByteBuf dump.
        return "SimpleQueryMessage{sql=<hidden>}";
    }
}
