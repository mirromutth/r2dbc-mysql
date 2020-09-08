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

import dev.miku.r2dbc.mysql.util.VarIntUtils;
import dev.miku.r2dbc.mysql.ConnectionContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Send parameter long data for prepared statements, it should be used by LOB types. (e.g. BLOB, CLOB)
 * <p>
 * Note: it must be sent before {@link PreparedExecuteMessage}.
 */
public final class PreparedLargeDataMessage extends LargeClientMessage {

    private static final int MIN_SIZE = Byte.BYTES + Integer.BYTES + Short.BYTES + Byte.BYTES + Long.BYTES;

    private static final byte LARGE_DATA_FLAG = 0x18;

    private final int statementId;

    private final int parameterId;

    private final Publisher<ByteBuf> data;

    public PreparedLargeDataMessage(int statementId, int parameterId, Publisher<ByteBuf> data) {
        this.statementId = statementId;
        this.parameterId = parameterId;
        this.data = requireNonNull(data, "data must not be null");
    }

    @Override
    protected Publisher<ByteBuf> fragments(ByteBufAllocator allocator, ConnectionContext context) {
        return Flux.from(data).collectList().flatMapMany(values -> {
            int i = 0;
            int size = values.size();
            ByteBuf[] results = new ByteBuf[size + 1];
            long bytes = 0;

            for (; i < size; ++i) {
                bytes += (results[i + 1] = values.get(i)).readableBytes();
            }

            ByteBuf header = allocator.buffer(MIN_SIZE, MIN_SIZE);

            try {
                header.writeByte(LARGE_DATA_FLAG)
                    .writeIntLE(statementId)
                    .writeShortLE(parameterId);

                VarIntUtils.writeVarInt(header, bytes);
                results[0] = header;
                header = null;

                return Flux.fromArray(results);
            } finally {
                if (header != null) {
                    header.release();
                }
            }
        });
    }
}
