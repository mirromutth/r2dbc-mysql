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

import dev.miku.r2dbc.mysql.util.LeftPadding;
import reactor.core.Disposable;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

abstract class ActiveStatus extends LeftPadding {

    static final AtomicIntegerFieldUpdater<ActiveStatus> ACTIVE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ActiveStatus.class, "active");

    private volatile int active; // p8 first part

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
        Runnable runnable = queue.poll();

        if (runnable == null) {
            // Queue was empty, set it to inactive.
            ACTIVE_UPDATER.lazySet(this, 0);
        } else {
            runnable.run();
        }
    }

    /**
     * Submit an exchange task. If the queue is inactive, it will execute directly rather than queuing.
     * Otherwise it will be queuing.
     *
     * @param exchange the exchange task includes request messages sending and response messages processor.
     */
    void submit(Runnable exchange) {
        if (ACTIVE_UPDATER.compareAndSet(this, 0, 1)) {
            exchange.run();
        } else {
            // Prev task may be completing before queue offer, and queue maybe empty now,
            // so queue may be inactive now.
            if (!queue.offer(exchange)) {
                throw new IllegalStateException("Request queue is full");
            }

            // Try execute if queue is inactive.
            if (ACTIVE_UPDATER.compareAndSet(this, 0, 1)) {
                run();
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
        ACTIVE_UPDATER.lazySet(this, 1);
        queue.clear();
    }
}
