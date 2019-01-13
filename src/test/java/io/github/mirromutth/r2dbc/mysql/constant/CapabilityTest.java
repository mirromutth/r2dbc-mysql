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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Test cases for {@link Capability}
 */
class CapabilityTest {

    /**
     * Make sure all flag values ​​are integral exponent times of 2,
     * otherwise some flag-based operations will have incorrect results.
     */
    @Test
    void multiples() {
        for (Capability capability : Capability.values()) {
            assertNotEquals(capability.getFlag(), 0);
            assertEquals(capability.getFlag(), Integer.lowestOneBit(capability.getFlag()));
        }
    }

    /**
     * Make sure all flag values are unique.
     */
    @Test
    void uniqueFlags() {
        Capability[] capabilities = Capability.values();
        Set<Integer> flags = new HashSet<>(capabilities.length << 1);

        for (Capability capability : capabilities) {
            assertFalse(flags.contains(capability.getFlag()));
            flags.add(capability.getFlag());
        }
    }
}
