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
 * A message for fetches the requested amount of rows produced by cursored {@link PreparedExecuteMessage}.
 * <p>
 * Note: last request must be {@link PreparedExecuteMessage} and it must be cursored.
 */
public final class PreparedFetchMessage extends SizedClientMessage {

    private static final int SIZE = Byte.BYTES + Integer.BYTES + Integer.BYTES;

    /**
     * In MySQL documentations, it may not be this value, but {@literal 0x1c} was correct.
     */
    private static final byte FETCH_FLAG = 0x1c;

    private final int statementId;

    private final int fetchSize;

    public PreparedFetchMessage(int statementId, int fetchSize) {
        this.statementId = statementId;
        this.fetchSize = fetchSize;
    }

    @Override
    protected int size() {
        return SIZE;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeByte(FETCH_FLAG).writeIntLE(statementId).writeIntLE(fetchSize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PreparedFetchMessage)) {
            return false;
        }

        PreparedFetchMessage that = (PreparedFetchMessage) o;

        return statementId == that.statementId && fetchSize == that.fetchSize;
    }

    @Override
    public int hashCode() {
        return 31 * statementId + fetchSize;
    }

    @Override
    public String toString() {
        return "PreparedFetchMessage{statementId=" + statementId +
            ", fetchSize=" + Integer.toUnsignedLong(fetchSize) + '}';
    }
}
