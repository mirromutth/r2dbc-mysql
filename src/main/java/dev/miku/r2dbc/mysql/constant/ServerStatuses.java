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

package dev.miku.r2dbc.mysql.constant;

/**
 * MySQL server's statuses flag code.
 */
public final class ServerStatuses {

    public static final short IN_TRANSACTION = 1;

    public static final short AUTO_COMMIT = 2;

    public static final short MORE_RESULTS_EXISTS = 8;

//    public static final short QUERY_NO_GOOD_INDEX_USED = 16;
//    public static final short QUERY_NO_INDEX_USED = 32;
//    public static final short CURSOR_EXISTS = 64;
//    public static final short LAST_ROW_SENT = 128;
//    public static final short DB_DROPPED = 256;
//    public static final short NO_BACKSLASH_ESCAPES = 512;
//    public static final short METADATA_CHANGED = 1024;
//    public static final short QUERY_WAS_SLOW = 2048;
//    public static final short PS_OUT_PARAMS = 4096;
//    public static final short IN_TRANS_READONLY = 8192;
//    public static final short SESSION_STATE_CHANGED = 16384;

    private ServerStatuses() {
    }
}
