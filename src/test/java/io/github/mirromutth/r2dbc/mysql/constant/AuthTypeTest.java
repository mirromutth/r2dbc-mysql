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
 * Test cases for {@link AuthType}
 */
class AuthTypeTest {

    /**
     * Make sure it has no "mysql_old_password"
     */
    @Test
    void noOldPassword() {
        for (AuthType type : AuthType.values()) {
            assertNotEquals(type.getNativeName(), "mysql_old_password");
        }
    }

    /**
     * Make sure all native names are unique.
     */
    @Test
    void uniqueNativeNames() {
        AuthType[] types = AuthType.values();
        Set<String> flags = new HashSet<>(types.length << 1);

        for (AuthType capability : types) {
            assertFalse(flags.contains(capability.getNativeName()));
            flags.add(capability.getNativeName());
        }
    }
}
