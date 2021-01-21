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

import java.util.regex.Pattern;

/**
 * A utility for matching host/address.
 */
public final class AddressUtils {

    private static final Pattern IPV4_PATTERN = Pattern.compile(
        "^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.)" +
            "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){2}" +
            "([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");

    private static final Pattern IPV6_PATTERN = Pattern.compile("^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$");

    private static final Pattern IPV6_COMPRESSED_PATTERN = Pattern.compile(
        "^(([0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){0,5})?)::(([0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){0,5})?)$");

    private static final int IPV6_COLONS = 7;

    /**
     * Checks if the host is an address of IP version 4.
     *
     * @param host the host should be check.
     * @return if is IPv4.
     */
    public static boolean isIpv4(String host) {
        // TODO: Use faster matches instead of regex.
        return IPV4_PATTERN.matcher(host).matches();
    }

    /**
     * Checks if the host is an address of IP version 6.
     *
     * @param host the host should be check.
     * @return if is IPv6.
     */
    public static boolean isIpv6(String host) {
        // TODO: Use faster matches instead of regex.
        return IPV6_PATTERN.matcher(host).matches() || isIpv6Compressed(host);
    }

    private static boolean isIpv6Compressed(String host) {
        int length = host.length();
        int colons = 0;

        for (int i = 0; i < length; ++i) {
            if (host.charAt(i) == ':') {
                ++colons;
            }
        }

        // TODO: Use faster matches instead of regex.
        return colons <= IPV6_COLONS && IPV6_COMPRESSED_PATTERN.matcher(host).matches();
    }

    private AddressUtils() { }
}
