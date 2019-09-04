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

package io.github.mirromutth.r2dbc.mysql.client;

import io.github.mirromutth.r2dbc.mysql.internal.LeftPadding;
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

    void submit(Runnable exchange) {
        if (ACTIVE_UPDATER.compareAndSet(this, 0, 1)) {
            exchange.run();
        } else {
            if (!queue.offer(exchange)) {
                throw new IllegalStateException("Request queue is full");
            }

            if (ACTIVE_UPDATER.compareAndSet(this, 0, 1)) {
                run();
            }
        }
    }

    long keeping(int v) {
        return p0 = p1 = p2 = p3 = p4 = p5 = p6 = p7 = p9 = pa = pb = pc = pd = pe = pf = p8 = v;
    }
}
