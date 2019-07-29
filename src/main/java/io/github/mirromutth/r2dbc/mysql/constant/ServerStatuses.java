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
 * MySQL server's statuses flag code.
 */
public final class ServerStatuses {

//    public static final int IN_TRANSACTION = 1;

    public static final int AUTO_COMMIT = 2;

//    public static final int MORE_RESULTS_EXISTS = 8;
//    public static final int QUERY_NO_GOOD_INDEX_USED = 16;
//    public static final int QUERY_NO_INDEX_USED = 32;
//    public static final int CURSOR_EXISTS = 64;
//    public static final int LAST_ROW_SENT = 128;
//    public static final int DB_DROPPED = 256;
//    public static final int NO_BACKSLASH_ESCAPES = 512;
//    public static final int METADATA_CHANGED = 1024;
//    public static final int QUERY_WAS_SLOW = 2048;
//    public static final int PS_OUT_PARAMS = 4096;
//    public static final int IN_TRANS_READONLY = 8192;
//    public static final int SESSION_STATE_CHANGED = 16384;

    private ServerStatuses() {
    }
}
