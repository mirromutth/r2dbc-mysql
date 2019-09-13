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
 * Column definition flags.
 */
public final class ColumnDefinitions {

    public static final int NOT_NULL = 1;

//    public static final int PRIMARY_PART = 1 << 1; // a part of the primary key
//    public static final int UNIQUE_PART = 1 << 2; // a part of a unique key
//    public static final int KEY_PART = 1 << 3; // a part of a normal key

    public static final int BLOB = 1 << 4;

    public static final int UNSIGNED = 1 << 5; // useful for integer types, like: use `BigInteger` for BIGINT UNSIGNED.

//    public static final int ZEROFILL = 1 << 6;
//    public static final int BINARY = 1 << 7;

    public static final int ENUMERABLE = 1 << 8; // type is enum

//    public static final int AUTO_INCREMENT = 1 << 9;
//    public static final int TIMESTAMP = 1 << 10;

    public static final int SET = 1 << 11; // type is set

//    public static final int NO_DEFAULT = 1 << 12; // column has no default value
//    public static final int ON_UPDATE_NOW = 1 << 13; // field will be set to NOW() in UPDATE statement

    // more flags are useless

    private ColumnDefinitions() {
    }
}
