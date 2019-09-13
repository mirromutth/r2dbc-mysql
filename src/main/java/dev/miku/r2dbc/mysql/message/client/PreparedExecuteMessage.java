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

import dev.miku.r2dbc.mysql.constant.CursorTypes;
import dev.miku.r2dbc.mysql.internal.AssertUtils;
import dev.miku.r2dbc.mysql.internal.ConnectionContext;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

/**
 * A message to execute a prepared statement once with parameter.
 */
public final class PreparedExecuteMessage extends LargeClientMessage implements ExchangeableMessage {

    private static final int NO_PARAM_SIZE = Byte.BYTES + Integer.BYTES + Byte.BYTES + Integer.BYTES;

    private static final byte EXECUTE_FLAG = 0x17;

    private static final byte CURSOR_TYPE = CursorTypes.NO_CURSOR;

    private static final int TIMES = 1;

    private final int statementId;

    private final ParameterValue[] parameters;

    public PreparedExecuteMessage(int statementId, ParameterValue[] parameters) {
        this.statementId = statementId;
        this.parameters = AssertUtils.requireNonNull(parameters, "parameters must not be null");
    }

    @Override
    public String toString() {
        return String.format("PreparedExecuteMessage{statementId=%d, has %d parameters}", statementId, parameters.length);
    }

    @Override
    protected Publisher<ByteBuf> fragments(ByteBufAllocator allocator, ConnectionContext context) {
        int size = parameters.length;
        ByteBuf buf;

        if (size == 0) {
            buf = allocator.buffer(NO_PARAM_SIZE, NO_PARAM_SIZE);
        } else {
            buf = allocator.buffer();
        }

        try {
            buf.writeByte(EXECUTE_FLAG)
                .writeIntLE(statementId)
                .writeByte(CURSOR_TYPE)
                .writeIntLE(TIMES);

            if (size == 0) {
                return Mono.just(buf);
            }

            List<ParameterValue> nonNull = new ArrayList<>(size);
            byte[] nullMap = fillNullBitmap(size, nonNull);

            // Fill null-bitmap.
            buf.writeBytes(nullMap);

            if (nonNull.isEmpty()) {
                // No need rebound.
                buf.writeBoolean(false);
                return Mono.just(buf);
            }

            buf.writeBoolean(true);
            writeTypes(buf, size);

            ParameterWriter writer = new ParameterWriter(buf);
            return Flux.fromArray(parameters)
                .concatMap(param -> param.writeTo(writer))
                .doOnError(e -> {
                    writer.dispose();
                    cancelParameters();
                })
                .thenMany(writer.publish());
        } catch (Throwable e) {
            buf.release();
            cancelParameters();
            return Mono.error(e);
        }
    }

    private byte[] fillNullBitmap(int size, List<ParameterValue> nonNull) {
        byte[] nullMap = new byte[ceilDiv8(size)];

        for (int i = 0; i < size; ++i) {
            ParameterValue value = parameters[i];

            if (value.isNull()) {
                nullMap[i >> 3] |= 1 << (i & 7);
            } else {
                nonNull.add(value);
            }
        }

        return nullMap;
    }

    private void writeTypes(ByteBuf buf, int size) {
        for (int i = 0; i < size; ++i) {
            buf.writeShortLE(parameters[i].getType());
        }
    }

    private void cancelParameters() {
        for (ParameterValue parameter : parameters) {
            parameter.cancel();
        }
    }

    private static int ceilDiv8(int x) {
        int r = x >> 3;

        if ((r << 3) == x) {
            return r;
        }

        return r + 1;
    }
}
