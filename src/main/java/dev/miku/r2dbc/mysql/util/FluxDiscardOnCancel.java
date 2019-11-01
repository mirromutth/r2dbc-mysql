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
 * A decorating operator that replays signals from its source to a {@link DiscardOnCancelSubscriber} and drains the
 * source upon {@link Subscription#cancel() cancel} and drops data signals until termination.
 * Draining data is required to complete a particular request/response window and clear the protocol
 * state as client code expects to start a request/response conversation without any previous
 * response state.
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
        this.source.subscribe(createSubscriber(actual, false));
    }

    @SuppressWarnings("unchecked")
    static <T> CoreSubscriber<T> createSubscriber(CoreSubscriber<? super T> s, boolean fuseable) {
        if (fuseable) {
            if (s instanceof Fuseable.ConditionalSubscriber) {
                return new DiscardOnCancelFuseableConditionalSubscriber<>((Fuseable.ConditionalSubscriber<? super T>) s);
            }
            return new DiscardOnCancelFuseableSubscriber<>(s);
        }

        if (s instanceof Fuseable.ConditionalSubscriber) {
            return new DiscardOnCancelConditionalSubscriber<>((Fuseable.ConditionalSubscriber<? super T>) s);
        }
        return new DiscardOnCancelSubscriber<>(s);
    }

    private static final class DiscardOnCancelSubscriber<T>
        extends AbstractSubscriber<T, Subscription, CoreSubscriber<? super T>> {

        DiscardOnCancelSubscriber(CoreSubscriber<? super T> actual) {
            super(actual);
        }
    }

    private static final class DiscardOnCancelFuseableSubscriber<T>
        extends AbstractFuseableSubscriber<T, CoreSubscriber<? super T>> {

        DiscardOnCancelFuseableSubscriber(CoreSubscriber<? super T> actual) {
            super(actual);
        }
    }

    private static final class DiscardOnCancelConditionalSubscriber<T>
        extends AbstractSubscriber<T, Subscription, Fuseable.ConditionalSubscriber<? super T>>
        implements Fuseable.ConditionalSubscriber<T> {

        DiscardOnCancelConditionalSubscriber(Fuseable.ConditionalSubscriber<? super T> actual) {
            super(actual);
        }

        @Override
        public boolean tryOnNext(T t) {
            if (get()) {
                Operators.onDiscard(t, this.ctx);
                return true;
            } else {
                return this.actual.tryOnNext(t);
            }
        }
    }

    private static final class DiscardOnCancelFuseableConditionalSubscriber<T>
        extends AbstractFuseableSubscriber<T, Fuseable.ConditionalSubscriber<? super T>>
        implements Fuseable.ConditionalSubscriber<T> {

        DiscardOnCancelFuseableConditionalSubscriber(Fuseable.ConditionalSubscriber<? super T> actual) {
            super(actual);
        }

        @Override
        public boolean tryOnNext(T t) {
            if (get()) {
                Operators.onDiscard(t, this.ctx);
                return true;
            } else {
                return this.actual.tryOnNext(t);
            }
        }
    }

    private static abstract class AbstractSubscriber<T, S extends Subscription, A extends CoreSubscriber<? super T>>
        extends AtomicBoolean implements CoreSubscriber<T>, Scannable, Subscription {

        final A actual;

        final Context ctx;

        S s;

        AbstractSubscriber(A actual) {
            this.actual = actual;
            this.ctx = actual.currentContext();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = (S) s;
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
            if (compareAndSet(false, true)) {
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
    }

    private static abstract class AbstractFuseableSubscriber<T, A extends CoreSubscriber<? super T>>
        extends AbstractSubscriber<T, Fuseable.QueueSubscription<T>, A>
        implements Fuseable.QueueSubscription<T> {

        int sourceMode;

        AbstractFuseableSubscriber(A actual) {
            super(actual);
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
