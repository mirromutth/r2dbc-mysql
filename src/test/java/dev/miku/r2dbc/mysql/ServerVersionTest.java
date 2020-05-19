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

package dev.miku.r2dbc.mysql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link ServerVersion}.
 */
class ServerVersionTest {

    @Test
    void parse() {
        ServerVersion v5_7_12 = ServerVersion.parse("5.7.12");
        ServerVersion v8 = ServerVersion.parse("8.0.0");
        ServerVersion v8_1 = ServerVersion.parse("8.1.0");
        ServerVersion v56_78_910 = ServerVersion.parse("56.78.910");
        ServerVersion v0 = ServerVersion.parse("");

        assertEquals(ServerVersion.create(0, 0, 0), ServerVersion.parse("-1"));
        assertEquals(ServerVersion.create(8, 0, 0), ServerVersion.parse("8.-1"));
        assertEquals(ServerVersion.create(0, 0, 0), ServerVersion.parse("-5.6.7"));
        assertEquals(ServerVersion.create(5, 0, 0), ServerVersion.parse("5.-6.7"));
        assertEquals(ServerVersion.create(5, 6, 0), ServerVersion.parse("5.6.-7"));

        assertEquals(v5_7_12, ServerVersion.create(5, 7, 12));
        assertEquals(v8, ServerVersion.create(8, 0, 0));
        assertEquals(v8_1, ServerVersion.create(8, 1, 0));
        assertEquals(v56_78_910, ServerVersion.create(56, 78, 910));
        assertEquals(v0, ServerVersion.create(0, 0, 0));

        assertEquals(v5_7_12, ServerVersion.parse("5.7.12.17"));
        assertEquals(v5_7_12, ServerVersion.parse("5.7.12-17"));
        assertEquals(v5_7_12, ServerVersion.parse("5.7.12.RELEASE"));
        assertEquals(v5_7_12, ServerVersion.parse("5.7.12.RC2"));
        assertEquals(v5_7_12, ServerVersion.parse("5.7.12RC2"));
        assertEquals(v5_7_12, ServerVersion.parse("5.7.12-v2"));
        assertEquals(v5_7_12, ServerVersion.parse("5.7.12-RC2"));

        assertEquals(v8, ServerVersion.parse("8"));
        assertEquals(v8, ServerVersion.parse("8-2"));
        assertEquals(v8, ServerVersion.parse("8v2"));
        assertEquals(v8, ServerVersion.parse("8-v2"));
        assertEquals(v8, ServerVersion.parse("8.0"));
        assertEquals(v8, ServerVersion.parse("8.0-2"));
        assertEquals(v8, ServerVersion.parse("8.0v2"));
        assertEquals(v8, ServerVersion.parse("8.0.0-2"));
        assertEquals(v8, ServerVersion.parse("8.0.0v2"));
        assertEquals(v8, ServerVersion.parse("8.0.0-v2"));

        assertEquals(v8_1, ServerVersion.parse("8.1"));
        assertEquals(v8_1, ServerVersion.parse("8.1-2"));
        assertEquals(v8_1, ServerVersion.parse("8.1v2"));
        assertEquals(v8_1, ServerVersion.parse("8.1-v2"));
        assertEquals(v8_1, ServerVersion.parse("8.1.0-2"));
        assertEquals(v8_1, ServerVersion.parse("8.1.0v2"));
        assertEquals(v8_1, ServerVersion.parse("8.1.0-v2"));

        assertEquals(v56_78_910, ServerVersion.parse("56.78.910.17"));
        assertEquals(v56_78_910, ServerVersion.parse("56.78.910-12"));
        assertEquals(v56_78_910, ServerVersion.parse("56.78.910.RELEASE"));
        assertEquals(v56_78_910, ServerVersion.parse("56.78.910.RC2"));
        assertEquals(v56_78_910, ServerVersion.parse("56.78.910RC2"));
        assertEquals(v56_78_910, ServerVersion.parse("56.78.910-v2"));
        assertEquals(v56_78_910, ServerVersion.parse("56.78.910-RC2"));

        assertEquals(ServerVersion.parse("5.7.12.17").toString(), "5.7.12.17");
        assertEquals(ServerVersion.parse("5.7.12-17").toString(), "5.7.12-17");
        assertEquals(ServerVersion.parse("5.7.12.RELEASE").toString(), "5.7.12.RELEASE");
        assertEquals(ServerVersion.parse("5.7.12.RC2").toString(), "5.7.12.RC2");
        assertEquals(ServerVersion.parse("5.7.12RC2").toString(), "5.7.12RC2");
        assertEquals(ServerVersion.parse("5.7.12-v2").toString(), "5.7.12-v2");
        assertEquals(ServerVersion.parse("5.7.12-RC2").toString(), "5.7.12-RC2");

        assertEquals(ServerVersion.parse("8").toString(), "8");
        assertEquals(ServerVersion.parse("8-2").toString(), "8-2");
        assertEquals(ServerVersion.parse("8v2").toString(), "8v2");
        assertEquals(ServerVersion.parse("8-v2").toString(), "8-v2");
        assertEquals(ServerVersion.parse("8.0").toString(), "8.0");
        assertEquals(ServerVersion.parse("8.0-2").toString(), "8.0-2");
        assertEquals(ServerVersion.parse("8.0v2").toString(), "8.0v2");
        assertEquals(ServerVersion.parse("8.0.0-2").toString(), "8.0.0-2");
        assertEquals(ServerVersion.parse("8.0.0v2").toString(), "8.0.0v2");
        assertEquals(ServerVersion.parse("8.0.0-v2").toString(), "8.0.0-v2");

        assertEquals(ServerVersion.parse("8.1").toString(), "8.1");
        assertEquals(ServerVersion.parse("8.1-2").toString(), "8.1-2");
        assertEquals(ServerVersion.parse("8.1v2").toString(), "8.1v2");
        assertEquals(ServerVersion.parse("8.1-v2").toString(), "8.1-v2");
        assertEquals(ServerVersion.parse("8.1.0-2").toString(), "8.1.0-2");
        assertEquals(ServerVersion.parse("8.1.0v2").toString(), "8.1.0v2");
        assertEquals(ServerVersion.parse("8.1.0-v2").toString(), "8.1.0-v2");

        assertEquals(ServerVersion.parse("56.78.910.17").toString(), "56.78.910.17");
        assertEquals(ServerVersion.parse("56.78.910-12").toString(), "56.78.910-12");
        assertEquals(ServerVersion.parse("56.78.910.RELEASE").toString(), "56.78.910.RELEASE");
        assertEquals(ServerVersion.parse("56.78.910.RC2").toString(), "56.78.910.RC2");
        assertEquals(ServerVersion.parse("56.78.910RC2").toString(), "56.78.910RC2");
        assertEquals(ServerVersion.parse("56.78.910-v2").toString(), "56.78.910-v2");
        assertEquals(ServerVersion.parse("56.78.910-RC2").toString(), "56.78.910-RC2");
    }
}
