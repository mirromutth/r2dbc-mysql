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

package dev.miku.r2dbc.mysql.util;

import dev.miku.r2dbc.mysql.constant.Envelopes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Operator utility.
 * <p>
 * This is a slightly altered version of R2DBC SQL Server's implementation:
 * https://github.com/r2dbc/r2dbc-mssql
 */
public final class OperatorUtils {

    /**
     * Replay signals from {@link Flux the source} until cancellation. Drains the source for data signals if the subscriber cancels the subscription.
     * <p>
     * Draining data is required to complete a particular request/response window and clear the protocol state as client code expects to start a
     * request/response conversation without leaving previous frames on the stack.
     *
     * @param source the source to decorate.
     * @param <T>    The type of values in both source and output sequences.
     * @return decorated {@link Flux}.
     * @throws IllegalArgumentException if {@code source} is {@code null}.
     */
    public static <T> Flux<T> discardOnCancel(Flux<? extends T> source) {
        requireNonNull(source, "source must not be null");

        if (source instanceof Fuseable) {
            return new FluxDiscardOnCancelFuseable<>(source);
        }

        return new FluxDiscardOnCancel<>(source);
    }

    public static Flux<ByteBuf> cumulateEnvelope(Flux<? extends ByteBuf> source, ByteBufAllocator allocator, int envelopeIdStart) {
        requireNonNull(source, "source must not be null");
        requireNonNull(allocator, "allocator must not be null");

        return new FluxCumulateEnvelope(source, allocator, Envelopes.MAX_ENVELOPE_SIZE, envelopeIdStart & 0xFF);
    }

    private OperatorUtils() {
    }
}
