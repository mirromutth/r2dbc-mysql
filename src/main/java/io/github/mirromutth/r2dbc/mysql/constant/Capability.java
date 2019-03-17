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

    // WARNING: should ALWAYS enable this option before newer protocol comes out
    PROTOCOL_41(512),
    INTERACTIVE(1024),
    SSL(2048),
    TRANSACTIONS(8192),
    RESERVED(16384),
    SECURE_CONNECTION(32768),
    MULTI_STATEMENTS(65536),
    MULTI_RESULTS(1 << 17),
    PS_MULTI_RESULTS(1 << 18),
    PLUGIN_AUTH(1 << 19),
    CONNECT_ATTRS(1 << 20),
    PLUGIN_AUTH_VAR_INT_SIZED_DATA(1 << 21), // can use var int sized bytes to encode client data
    CAN_HANDLE_EXPIRED_PASSWORD(1 << 22),
    SESSION_TRACK(1 << 23),

    // WARNING: should ALWAYS enable this option. MySQL recommends deprecating EOF messages
    DEPRECATE_EOF(1 << 24),
    // WARNING: means server MAYBE have NOT metadata in response, should NEVER enable this option
    OPTIONAL_RESULT_SET_METADATA(1 << 25),

    SSL_VERIFY_SERVER_CERT(1 << 30),
    REMEMBER_OPTIONS(1 << 31);

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
