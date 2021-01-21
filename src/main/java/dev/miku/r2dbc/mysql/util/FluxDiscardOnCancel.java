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

package dev.miku.r2dbc.mysql.util;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

/**
 * A decorating operator that replays signals from its source to a {@link DiscardOnCancelSubscriber} and
 * drains the source upon {@link Subscription#cancel() cancel} and drops data signals until termination.
 * Draining data is required to complete a particular request/response window and clear the protocol state as
 * client code expects to start a request/response conversation without any previous response state.
 * <p>
 * This is a slightly altered version of R2DBC SQL Server's implementation:
 * https://github.com/r2dbc/r2dbc-mssql
 */
final class FluxDiscardOnCancel<T> extends FluxOperator<T, T> {

    FluxDiscardOnCancel(Flux<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        this.source.subscribe(DiscardOnCancelSubscriber.create(actual, false));
    }
}
