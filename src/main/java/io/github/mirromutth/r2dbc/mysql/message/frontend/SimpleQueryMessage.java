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

package io.github.mirromutth.r2dbc.mysql.message.frontend;

import io.github.mirromutth.r2dbc.mysql.constant.CommandType;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * The message include a plain text SQL query without any parameter.
 */
public final class SimpleQueryMessage extends AbstractFrontendMessage implements CommandMessage {

    private static final byte QUERY_FLAG = 3;

    private final String sql;

    public SimpleQueryMessage(String sql) {
        this.sql = requireNonNull(sql, "sql must not be null");
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.STATEMENT_SIMPLE;
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
}
