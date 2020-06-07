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
import dev.miku.r2dbc.mysql.constant.CursorTypes;
import dev.miku.r2dbc.mysql.util.OperatorUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A message to execute a prepared statement once with parameter.
 */
public final class PreparedExecuteMessage extends LargeClientMessage implements ExchangeableMessage, Disposable {

    private static final int NO_PARAM_SIZE = Byte.BYTES + Integer.BYTES + Byte.BYTES + Integer.BYTES;

    private static final int TIMES = 1;

    private static final byte EXECUTE_FLAG = 0x17;

    private static final Consumer<Parameter> DISPOSE = Parameter::dispose;

    private final int statementId;

    /**
     * Immediate execute once instead of fetch.
     */
    private final boolean immediate;

    private final Parameter[] values;

    public PreparedExecuteMessage(int statementId, boolean immediate, Parameter[] values) {
        this.values = requireNonNull(values, "values must not be null");
        this.statementId = statementId;
        this.immediate = immediate;
    }

    @Override
    public void dispose() {
        for (Parameter value : values) {
            value.dispose();
        }
        Arrays.fill(values, null);
    }

    @Override
    public String toString() {
        return String.format("PreparedExecuteMessage{statementId=%d, immediate=%b, has %d parameters}", statementId, immediate, values.length);
    }

    @Override
    protected Publisher<ByteBuf> fragments(ByteBufAllocator allocator, ConnectionContext context) {
        int size = values.length;
        ByteBuf buf;

        if (size == 0) {
            buf = allocator.buffer(NO_PARAM_SIZE, NO_PARAM_SIZE);
        } else {
            buf = allocator.buffer();
        }

        try {
            buf.writeByte(EXECUTE_FLAG)
                .writeIntLE(statementId)
                .writeByte(immediate ? CursorTypes.NO_CURSOR : CursorTypes.READ_ONLY)
                .writeIntLE(TIMES);

            if (size == 0) {
                return Mono.just(buf);
            }

            List<Parameter> nonNull = new ArrayList<>(size);
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

            Flux<ByteBuf> parameters = OperatorUtils.discardOnCancel(Flux.fromArray(values))
                .doOnDiscard(Parameter.class, DISPOSE)
                .concatMap(Parameter::publishBinary);

            return Flux.just(buf).concatWith(parameters);
        } catch (Throwable e) {
            buf.release();
            cancelParameters();
            return Mono.error(e);
        }
    }

    private byte[] fillNullBitmap(int size, List<Parameter> nonNull) {
        byte[] nullMap = new byte[ceilDiv8(size)];

        for (int i = 0; i < size; ++i) {
            Parameter value = values[i];

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
            buf.writeShortLE(values[i].getType());
        }
    }

    private void cancelParameters() {
        for (Parameter value : values) {
            value.dispose();
        }
    }

    private static int ceilDiv8(int x) {
        int r = x >> 3;
        return (r << 3) == x ? r : r + 1;
    }
}
