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

package dev.miku.r2dbc.mysql.util;

import reactor.core.publisher.Flux;

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
     */
    public static <T> Flux<T> discardOnCancel(Flux<? extends T> source) {
        return new FluxDiscardOnCancel<>(source);
    }

    private OperatorUtils() {
    }
}
