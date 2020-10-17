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

import io.netty.buffer.ByteBuf;

/**
 * A message that close the prepared statement specified by id.
 */
public final class PreparedCloseMessage extends SizedClientMessage {

    private static final int SIZE = Byte.BYTES + Integer.BYTES;

    private static final byte CLOSE_FLAG = 0x19;

    private final int statementId;

    public PreparedCloseMessage(int statementId) {
        this.statementId = statementId;
    }

    @Override
    protected int size() {
        return SIZE;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeByte(CLOSE_FLAG).writeIntLE(statementId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PreparedCloseMessage)) {
            return false;
        }

        PreparedCloseMessage that = (PreparedCloseMessage) o;

        return statementId == that.statementId;
    }

    @Override
    public int hashCode() {
        return statementId;
    }

    @Override
    public String toString() {
        return "PreparedCloseMessage{statementId=" + statementId + '}';
    }
}
