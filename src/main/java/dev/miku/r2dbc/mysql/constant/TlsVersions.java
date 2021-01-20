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

package dev.miku.r2dbc.mysql.constant;

/**
 * All TLS protocol version names supported by MySQL.
 */
public final class TlsVersions {

    /**
     * TLS version 1.0, not recommended. All available MySQL versions support it.
     */
    public static final String TLS1 = "TLSv1";

    /**
     * TLS version 1.1. All available MySQL versions support it.
     */
    public static final String TLS1_1 = "TLSv1.1";

    /**
     * TLS version 1.2. It is supported in 8.0.4 (or higher) Community Edition, or 5.6.0 (or higher)
     * Enterprise Edition.
     * <p>
     * Note: The Enterprise Edition version will looks like {@literal 5.7.26-enterprise}.
     */
    public static final String TLS1_2 = "TLSv1.2";

    /**
     * TLS version 1.3. It is available as of MySQL 8.0.16 or higher, and requires compiling MySQL using
     * OpenSSL 1.1.1 or higher.
     * <p>
     * It will not be used unless user has set it up, because the driver cannot know which compiling library
     * the MySQL server uses.
     */
    public static final String TLS1_3 = "TLSv1.3";

    private TlsVersions() { }
}
