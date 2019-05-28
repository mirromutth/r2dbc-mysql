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

import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * The message tell MySQL server to close the statement specified by id.
 */
public final class CloseStatementMessage extends AbstractClientMessage {

    private static final byte STATEMENT_CLOSE_FLAG = 0x19;

    private final int statementId;

    public CloseStatementMessage(int statementId) {
        this.statementId = statementId;
    }

    @Override
    public boolean resetSequenceId() {
        return false;
    }

    @Override
    protected ByteBuf encodeSingle(ByteBufAllocator bufAllocator, MySqlSession session) {
        final ByteBuf buf = bufAllocator.buffer();

        try {
            return buf.writeByte(STATEMENT_CLOSE_FLAG).writeIntLE(statementId);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }
}
