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

import io.netty.buffer.ByteBuf;

/**
 * A message that reset the prepared statement specified by id.
 */
public final class PreparedResetMessage extends SizedClientMessage {

    private static final int SIZE = Byte.BYTES + Integer.BYTES;

    private static final byte RESET_FLAG = 0x1A;

    private final int statementId;

    public PreparedResetMessage(int statementId) {
        this.statementId = statementId;
    }

    @Override
    protected int size() {
        return SIZE;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeByte(RESET_FLAG).writeIntLE(statementId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PreparedResetMessage that = (PreparedResetMessage) o;

        return statementId == that.statementId;
    }

    @Override
    public int hashCode() {
        return statementId;
    }

    @Override
    public String toString() {
        return "PreparedResetMessage{statementId=" + statementId + '}';
    }
}
