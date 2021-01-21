/*
 * Copyright 2018-2021 the original author or authors.
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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link AddressUtils}.
 */
class AddressUtilsTest {

    @Test
    void isIpv4() {
        assertTrue(AddressUtils.isIpv4("1.0.0.0"));
        assertTrue(AddressUtils.isIpv4("127.0.0.1"));
        assertTrue(AddressUtils.isIpv4("10.11.12.13"));
        assertTrue(AddressUtils.isIpv4("192.168.0.0"));
        assertTrue(AddressUtils.isIpv4("255.255.255.255"));

        assertFalse(AddressUtils.isIpv4("0.0.0.0"));
        assertFalse(AddressUtils.isIpv4(" 127.0.0.1 "));
        assertFalse(AddressUtils.isIpv4("01.11.12.13"));
        assertFalse(AddressUtils.isIpv4("092.168.0.1"));
        assertFalse(AddressUtils.isIpv4("055.255.255.255"));
        assertFalse(AddressUtils.isIpv4("g.ar.ba.ge"));
        assertFalse(AddressUtils.isIpv4("192.168.0"));
        assertFalse(AddressUtils.isIpv4("192.168.0a.0"));
        assertFalse(AddressUtils.isIpv4("256.255.255.255"));
        assertFalse(AddressUtils.isIpv4("0.255.255.255"));

        assertFalse(AddressUtils.isIpv4("::"));
        assertFalse(AddressUtils.isIpv4("::1"));
        assertFalse(AddressUtils.isIpv4("0:0:0:0:0:0:0:0"));
        assertFalse(AddressUtils.isIpv4("0:0:0:0:0:0:0:1"));
        assertFalse(AddressUtils.isIpv4("2001:0acd:0000:0000:0000:0000:3939:21fe"));
        assertFalse(AddressUtils.isIpv4("2001:acd:0:0:0:0:3939:21fe"));
        assertFalse(AddressUtils.isIpv4("2001:0acd:0:0::3939:21fe"));
        assertFalse(AddressUtils.isIpv4("2001:0acd::3939:21fe"));
        assertFalse(AddressUtils.isIpv4("2001:acd::3939:21fe"));
    }

    @Test
    void isIpv6() {
        assertTrue(AddressUtils.isIpv6("::"));
        assertTrue(AddressUtils.isIpv6("::1"));
        assertTrue(AddressUtils.isIpv6("0:0:0:0:0:0:0:0"));
        assertTrue(AddressUtils.isIpv6("0:0:0:0:0:0:0:1"));
        assertTrue(AddressUtils.isIpv6("2001:0acd:0000:0000:0000:0000:3939:21fe"));
        assertTrue(AddressUtils.isIpv6("2001:acd:0:0:0:0:3939:21fe"));
        assertTrue(AddressUtils.isIpv6("2001:0acd:0:0::3939:21fe"));
        assertTrue(AddressUtils.isIpv6("2001:0acd::3939:21fe"));
        assertTrue(AddressUtils.isIpv6("2001:acd::3939:21fe"));

        assertFalse(AddressUtils.isIpv6(""));
        assertFalse(AddressUtils.isIpv6(":1"));
        assertFalse(AddressUtils.isIpv6("0:0:0:0:0:0:0"));
        assertFalse(AddressUtils.isIpv6("0:0:0:0:0:0:0:0:0"));
        assertFalse(AddressUtils.isIpv6("2001:0acd:0000:garb:age0:0000:3939:21fe"));
        assertFalse(AddressUtils.isIpv6("2001:0agd:0000:0000:0000:0000:3939:21fe"));
        assertFalse(AddressUtils.isIpv6("2001:0acd::0000::21fe"));
        assertFalse(AddressUtils.isIpv6("1:2:3:4:5:6:7::9"));
        assertFalse(AddressUtils.isIpv6("1::3:4:5:6:7:8:9"));
        assertFalse(AddressUtils.isIpv6("::3:4:5:6:7:8:9"));
        assertFalse(AddressUtils.isIpv6("1:2::4:5:6:7:8:9"));
        assertFalse(AddressUtils.isIpv6("1:2:3:4:5:6::8:9"));

        assertFalse(AddressUtils.isIpv6("0.0.0.0"));
        assertFalse(AddressUtils.isIpv6("1.0.0.0"));
        assertFalse(AddressUtils.isIpv6("127.0.0.1"));
        assertFalse(AddressUtils.isIpv6("10.11.12.13"));
        assertFalse(AddressUtils.isIpv6("192.168.0.0"));
        assertFalse(AddressUtils.isIpv6("255.255.255.255"));
    }
}
