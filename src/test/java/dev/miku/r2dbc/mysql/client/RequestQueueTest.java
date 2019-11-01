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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link RequestQueue}.
 */
class RequestQueueTest {

    @Test
    void submit() {
        RequestQueue queue = new RequestQueue();
        List<Integer> arr = new AddEventList(queue);

        queue.submit(() -> arr.add(1));
        queue.submit(() -> arr.add(2));
        queue.submit(() -> arr.add(3));

        assertEquals(arr, Arrays.asList(1, 2, 3));
    }

    @Test
    void dispose() {
        RequestQueue queue = new RequestQueue();
        List<Integer> arr = new AddEventList(queue);

        queue.submit(() -> arr.add(1));
        queue.submit(() -> arr.add(2));
        queue.dispose();
        queue.submit(() -> arr.add(3));
        queue.submit(() -> arr.add(4));

        assertEquals(arr, Arrays.asList(1, 2));
    }

    @Test
    void keeping() {
        RequestQueue queue = new RequestQueue();
        assertEquals(queue.keeping(1), 1L);
        assertEquals(queue.keeping(-1), -1L);
    }

    private static final class AddEventList extends ArrayList<Integer> {

        private final RequestQueue queue;

        private AddEventList(RequestQueue queue) {
            this.queue = queue;
        }

        @Override
        public boolean add(Integer o) {
            boolean result = super.add(o);
            // Mock request completed.
            queue.run();
            return result;
        }
    }
}
