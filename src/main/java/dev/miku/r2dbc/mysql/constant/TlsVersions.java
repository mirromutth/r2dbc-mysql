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

package dev.miku.r2dbc.mysql.constant;

/**
 * All TLS protocol version names supported by MySQL.
 */
public final class TlsVersions {

    /**
     * TLS version 1.0, not recommended. All available MySQL versions support it.
     * <p>
     * Notice: it has been disabled in the latest version of JDKs.
     */
    public static final String TLS1 = "TLSv1";

    /**
     * TLS version 1.1. All available MySQL versions support it.
     * <p>
     * Notice: it has been disabled in the latest version of JDKs.
     */
    public static final String TLS1_1 = "TLSv1.1";

    /**
     * TLS version 1.2. Supported in MySQL community edition 8.0, 5.7.28 and later, and 5.6.46 and later, and
     * for all enterprise editions of MySQL Servers.
     */
    public static final String TLS1_2 = "TLSv1.2";

    /**
     * TLS version 1.3. Supported in MySQL community edition 8.0, 5.7.28 and later, and 5.6.46 and later, and
     * for all enterprise editions of MySQL Servers.
     */
    public static final String TLS1_3 = "TLSv1.3";

    private TlsVersions() { }
}
