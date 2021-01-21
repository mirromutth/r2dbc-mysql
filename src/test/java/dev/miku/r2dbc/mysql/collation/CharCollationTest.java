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

package dev.miku.r2dbc.mysql.collation;

import dev.miku.r2dbc.mysql.ServerVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for {@link CharCollation}.
 */
class CharCollationTest {

    private static final ServerVersion version = ServerVersion.create(0, 0, 0);

    @Test
    void fromId() {
        assertNotNull(CharCollation.fromId(33, version)); // utf8 general case insensitivity
        assertNotNull(CharCollation.fromId(45, version)); // utf8mb4 general case insensitivity
        assertNotNull(CharCollation.fromId(224, version)); // utf8mb4 unicode case insensitivity
        assertNotNull(CharCollation.fromId(246, version)); // utf8mb4 unicode 5.20 case insensitivity
        // utf8mb4 unicode 9.00 accent insensitivity and case insensitivity
        assertNotNull(CharCollation.fromId(255, version));
        assertNotEquals(CharCollation.fromId(33, version), CharCollation.fromId(224, version));
    }
}
