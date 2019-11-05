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

package dev.miku.r2dbc.mysql.client;

import reactor.core.Disposable;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

abstract class LeftPadding {

    long p0, p1, p2, p3, p4, p5, p6, p7;
}

abstract class ActiveStatus extends LeftPadding {

    static final int DISPOSE = -1;

    static final int IDLE = 0;

    static final int ACTIVE = 1;

    static final AtomicIntegerFieldUpdater<ActiveStatus> STATUS_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ActiveStatus.class, "status");

    protected volatile int status = IDLE; // p8 first part

    int p8; // p8 second part

    long p9, pa, pb, pc, pd, pe, pf;
}

/**
 * Request queue to collect incoming exchange requests.
 * <p>
 * Submission conditionally queues requests if an ongoing exchange was active by the time of subscription.
 * Drains queued commands on exchange completion if there are queued commands or disable active flag.
 */
final class RequestQueue extends ActiveStatus implements Runnable {

    private final Queue<Runnable> queue = Queues.<Runnable>small().get();

    /**
     * Current exchange completed, refresh to next exchange or set to inactive.
     */
    @Override
    public void run() {
        Runnable exchange = queue.poll();

        if (exchange == null) {
            // Queue was empty, set it to idle if it is not disposed.
            STATUS_UPDATER.compareAndSet(this, ACTIVE, IDLE);
        } else {
            int status = this.status;

            if (status == DISPOSE) {
                // Disposed, release polled and should clear queue because don't sure it is empty.
                if (exchange instanceof Disposable) {
                    ((Disposable) exchange).dispose();
                }
                freeAll();
            } else {
                exchange.run();
            }
        }
    }

    /**
     * Submit an exchange task. If the queue is inactive, it will execute directly rather than queuing.
     * Otherwise it will be queuing.
     *
     * @param exchange the exchange task includes request messages sending and response messages processor.
     */
    void submit(Runnable exchange) {
        if (STATUS_UPDATER.compareAndSet(this, IDLE, ACTIVE)) {
            // Fast path for general way.
            exchange.run();
            return;
        }

        // Check dispose after fast path failed.
        int status = this.status;

        if (status == DISPOSE) {
            // Disposed and no need clear queue because it should be cleared by other one.
            if (exchange instanceof Disposable) {
                ((Disposable) exchange).dispose();
            }
            throw new IllegalStateException("Request queue was disposed");
        }

        // Prev task may be completing before queue offer, so queue may be idle now.
        if (!queue.offer(exchange)) {
            if (exchange instanceof Disposable) {
                ((Disposable) exchange).dispose();
            }
            // Even disposed now, no need clear queue because it should be cleared by other one.
            throw new IllegalStateException("Request queue is full");
        }

        if (STATUS_UPDATER.compareAndSet(this, IDLE, ACTIVE)) {
            // Try execute if queue is idle.
            run();
        } else {
            // Check dispose again after offer success and not idle.
            status = this.status;

            if (status == DISPOSE) {
                // Disposed, should clear queue because an element has just been offered to the queue.
                freeAll();
                throw new IllegalStateException("Request queue was disposed");
            }
        }
    }

    /**
     * Keep padding, maybe useful, maybe useless, whatever we should make sure padding
     * would not be reduced by compiler.
     *
     * @param v any value, because it is not matter
     * @return {@code v} self which type is {@code long}
     */
    long keeping(int v) {
        return p0 = p1 = p2 = p3 = p4 = p5 = p6 = p7 = p9 = pa = pb = pc = pd = pe = pf = p8 = v;
    }

    void dispose() {
        STATUS_UPDATER.set(this, DISPOSE);
        freeAll();
    }

    private void freeAll() {
        Runnable runnable;

        while ((runnable = queue.poll()) != null) {
            if (runnable instanceof Disposable) {
                ((Disposable) runnable).dispose();
            }
        }
    }
}
