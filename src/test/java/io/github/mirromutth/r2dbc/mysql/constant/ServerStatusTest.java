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

package io.github.mirromutth.r2dbc.mysql.constant;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for {@link ServerStatus}
 */
class ServerStatusTest {

    /**
     * Make sure all flag values ​​are integral exponent times of 2,
     * otherwise some flag-based operations will have incorrect results.
     */
    @Test
    void multiples() {
        for (ServerStatus status : ServerStatus.values()) {
            assertNotEquals(status.getFlag(), 0);
            assertEquals(status.getFlag(), Integer.lowestOneBit(status.getFlag()));
        }
    }

    /**
     * Make sure all flag values are unique.
     */
    @Test
    void uniqueFlags() {
        ServerStatus[] statuses = ServerStatus.values();
        Set<Integer> flags = new HashSet<>(statuses.length << 1);

        for (ServerStatus status : statuses) {
            assertFalse(flags.contains(status.getFlag()));
            flags.add(status.getFlag());
        }
    }
}
