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

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A decorating operator that replays signals from its source to a {@link DiscardOnCancelSubscriber} and drains the
 * source upon {@link Subscription#cancel() cancel} and drops data signals until termination.
 * Draining data is required to complete a particular request/response window and clear the protocol
 * state as client code expects to start a request/response conversation without any previous
 * response state.
 * <p>
 * This is a slightly altered version of R2DBC SQL Server's implementation:
 * https://github.com/r2dbc/r2dbc-mssql
 */
final class FluxDiscardOnCancel<T> extends Flux<T> {

    private static final Logger logger = LoggerFactory.getLogger(FluxDiscardOnCancel.class);

    private final Flux<? extends T> source;

    FluxDiscardOnCancel(Flux<? extends T> source) {
        this.source = source;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        this.source.subscribe(new DiscardOnCancelSubscriber<>(actual));
    }

    static final class DiscardOnCancelSubscriber<T> extends AtomicBoolean implements CoreSubscriber<T>, Subscription {

        final CoreSubscriber<T> actual;

        final Context ctx;

        Subscription s;

        DiscardOnCancelSubscriber(CoreSubscriber<T> actual) {
            this.actual = actual;
            this.ctx = actual.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;
                this.actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (get()) {
                Operators.onDiscard(t, this.ctx);
            } else {
                this.actual.onNext(t);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (get()) {
                Operators.onErrorDropped(t, this.ctx);
            } else {
                this.actual.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (!get()) {
                this.actual.onComplete();
            }
        }

        @Override
        public void request(long n) {
            this.s.request(n);
        }

        @Override
        public void cancel() {
            if (compareAndSet(false, true)) {
                logger.debug("Received cancel signal");
                this.s.request(Long.MAX_VALUE);
            }
        }
    }
}
