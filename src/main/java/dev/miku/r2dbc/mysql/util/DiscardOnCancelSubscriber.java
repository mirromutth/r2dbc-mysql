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

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link CoreSubscriber} for request all elements and drains/discards them from upstream
 * when downstream want to cancel its subscribe.
 * <p>
 * The drains/discards behavior is defined by {@code doOnDiscard}, and applied by {@link Operators#onDiscard}.
 */
class DiscardOnCancelSubscriber<T, S extends Subscription, A extends CoreSubscriber<? super T>> extends AtomicInteger
    implements CoreSubscriber<T>, Scannable, Subscription {

    private static final int TERMINATED = 2;

    private static final int CANCELLED = 1;

    final A actual;

    final Context ctx;

    S s;

    DiscardOnCancelSubscriber(A actual) {
        this.actual = actual;
        this.ctx = actual.currentContext();
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void onSubscribe(Subscription s) {
        if (Operators.validate(this.s, s)) {
            this.s = (S) s;
            this.actual.onSubscribe(this);
        }
    }

    @Override
    public final void onNext(T t) {
        if (get() == 0) {
            this.actual.onNext(t);
        } else {
            Operators.onDiscard(t, this.ctx);
        }
    }

    @Override
    public final void onError(Throwable t) {
        if (compareAndSet(0, TERMINATED)) {
            this.actual.onError(t);
        } else {
            Operators.onErrorDropped(t, this.ctx);
        }
    }

    @Override
    public final void onComplete() {
        if (compareAndSet(0, TERMINATED)) {
            this.actual.onComplete();
        }
    }

    @Override
    public final void request(long n) {
        this.s.request(n);
    }

    @Override
    public final void cancel() {
        if (compareAndSet(0, CANCELLED)) {
            this.s.request(Long.MAX_VALUE);
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public final Object scanUnsafe(Attr key) {
        if (key == Attr.PARENT) {
            return this.s;
        } else if (key == Attr.ACTUAL) {
            return this.actual;
        } else if (key == Attr.TERMINATED) {
            return get() == TERMINATED;
        } else if (key == Attr.CANCELLED) {
            return get() == CANCELLED;
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    static <T> CoreSubscriber<T> create(CoreSubscriber<? super T> s, boolean fuseable) {
        if (fuseable) {
            if (s instanceof Fuseable.ConditionalSubscriber) {
                return new DiscardOnCancelFuseableConditionalSubscriber<>((Fuseable.ConditionalSubscriber<? super T>) s);
            }
            return new DiscardOnCancelFuseableSubscriber<T, CoreSubscriber<? super T>>(s);
        }

        if (s instanceof Fuseable.ConditionalSubscriber) {
            return new DiscardOnCancelConditionalSubscriber<>((Fuseable.ConditionalSubscriber<? super T>) s);
        }
        return new DiscardOnCancelSubscriber<T, Subscription, CoreSubscriber<? super T>>(s);
    }
}

/**
 * An extension of {@link DiscardOnCancelSubscriber} for implements {@link Fuseable.QueueSubscription}.
 */
class DiscardOnCancelFuseableSubscriber<T, A extends CoreSubscriber<? super T>> extends DiscardOnCancelSubscriber<T, Fuseable.QueueSubscription<T>, A>
    implements Fuseable.QueueSubscription<T> {

    DiscardOnCancelFuseableSubscriber(A actual) {
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

        return this.s.requestFusion(modes);
    }

    @Override
    public int size() {
        return this.s.size();
    }
}

/**
 * An extension of {@link DiscardOnCancelSubscriber} for implements {@link Fuseable.ConditionalSubscriber}.
 */
final class DiscardOnCancelConditionalSubscriber<T> extends DiscardOnCancelSubscriber<T, Subscription, Fuseable.ConditionalSubscriber<? super T>>
    implements Fuseable.ConditionalSubscriber<T> {

    DiscardOnCancelConditionalSubscriber(Fuseable.ConditionalSubscriber<? super T> actual) {
        super(actual);
    }

    @Override
    public boolean tryOnNext(T t) {
        if (get() == 0) {
            return this.actual.tryOnNext(t);
        } else {
            Operators.onDiscard(t, this.ctx);
            return true;
        }
    }
}

/**
 * An extension of {@link DiscardOnCancelFuseableSubscriber} for implements {@link Fuseable.ConditionalSubscriber}.
 */
final class DiscardOnCancelFuseableConditionalSubscriber<T> extends DiscardOnCancelFuseableSubscriber<T, Fuseable.ConditionalSubscriber<? super T>>
    implements Fuseable.ConditionalSubscriber<T> {

    DiscardOnCancelFuseableConditionalSubscriber(Fuseable.ConditionalSubscriber<? super T> actual) {
        super(actual);
    }

    @Override
    public boolean tryOnNext(T t) {
        if (get() == 0) {
            return this.actual.tryOnNext(t);
        } else {
            Operators.onDiscard(t, this.ctx);
            return true;
        }
    }
}
