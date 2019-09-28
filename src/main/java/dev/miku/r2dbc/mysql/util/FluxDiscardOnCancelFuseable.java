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
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A decorating operator that replays signals from its source to a {@link DiscardOnCancelFuseableSubscriber} and drains the
 * source upon {@link Subscription#cancel() cancel} and drops data signals until termination.
 * Draining data is required to complete a particular request/response window and clear the protocol
 * state as client code expects to start a request/response conversation without any previous
 * response state.
 *
 * @see FluxDiscardOnCancel the version without {@link Fuseable}.
 */
final class FluxDiscardOnCancelFuseable<T> extends FluxOperator<T, T> implements Fuseable {

    FluxDiscardOnCancelFuseable(Flux<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        this.source.subscribe(new DiscardOnCancelFuseableSubscriber<>(actual));
    }

    private static final class DiscardOnCancelFuseableSubscriber<T> extends AtomicBoolean implements CoreSubscriber<T>, Scannable, QueueSubscription<T> {

        final CoreSubscriber<? super T> actual;

        final Context ctx;

        int sourceMode;

        QueueSubscription<T> s;

        DiscardOnCancelFuseableSubscriber(CoreSubscriber<T> actual) {
            this.actual = actual;
            this.ctx = actual.currentContext();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = (QueueSubscription<T>) s;
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
                this.s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public T poll() {
            try {
                return this.s.poll();
            } catch (Throwable e) {
                throw Exceptions.propagate(Operators.onOperatorError(this.s, e, this.actual.currentContext()));
            }
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) {
                return this.s;
            } else if (key == Attr.ACTUAL) {
                return this.actual;
            } else {
                return null;
            }
        }

        @Override
        public boolean isEmpty() {
            return this.s.isEmpty();
        }

        @Override
        public void clear() {
            this.s.clear();
        }

        @Override
        public int requestFusion(int modes) {
            if ((modes & Fuseable.THREAD_BARRIER) != 0) {
                return Fuseable.NONE;
            }

            int m = this.s.requestFusion(modes);

            this.sourceMode = m;

            return m;
        }

        @Override
        public int size() {
            return this.s.size();
        }
    }
}
