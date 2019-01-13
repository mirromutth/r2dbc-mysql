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

/**
 * Values for the capabilities flag bitmask used by the MySQL Client/Server Protocol.
 */
public enum Capability {

    LONG_PASSWORD(1), // new more secure passwords
    FOUND_ROWS(2),
    LONG_FLAG(4),
    CONNECT_WITH_DB(8),
    COMPRESS(32),
    LOCAL_FILES(128),
    PROTOCOL_41(512),
    INTERACTIVE(1024),
    SSL(2048),
    TRANSACTIONS(8192),
    RESERVED(16384),
    SECURE_CONNECTION(32768),
    MULTI_STATEMENTS(65536),
    MULTI_RESULTS(131072),
    PS_MULTI_RESULTS(262144),
    PLUGIN_AUTH(524288),
    CONNECT_ATTRS(1048576),
    PLUGIN_AUTH_VAR_INT_SIZED_DATA(2097152), // can use var int sized bytes to encode client data
    CAN_HANDLE_EXPIRED_PASSWORD(4194304),
    SESSION_TRACK(8388608),
    DEPRECATE_EOF(16777216);

    private final int flag;

    Capability(int flag) {
        this.flag = flag;
    }

    /**
     * Do NOT use it outer than {@code r2dbc-mysql}, because it is native flag code of MySQL,
     * we can NOT promise it will be never changes.
     *
     * @return the native flag code
     */
    public int getFlag() {
        return flag;
    }
}
