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
 * MySQL column/parameter data types.
 */
public final class DataTypes {

    public static final short DECIMAL = 0;

    public static final short TINYINT = 1;

    public static final short SMALLINT = 2;

    public static final short INT = 3;

    public static final short FLOAT = 4;

    public static final short DOUBLE = 5;

    public static final short NULL = 6;

    public static final short TIMESTAMP = 7;

    public static final short BIGINT = 8;

    public static final short MEDIUMINT = 9;

    public static final short DATE = 10;

    public static final short TIME = 11;

    public static final short DATETIME = 12;

    public static final short YEAR = 13;

    // NEW_DATE (14) is internal type of MySQL server, do NOT support this type.
    public static final short VARCHAR = 15;

    public static final short BIT = 16;

    public static final short TIMESTAMP2 = 17;

    // DATETIME2 (18) and TIME2 (19) are internal types, do NOT support them.
    public static final short JSON = 245;

    public static final short NEW_DECIMAL = 246;

    /**
     * Virtual type, MySQL server always returned ENUMERABLE flag in definitions and type is VARCHAR.
     */
    public static final short ENUMERABLE = 247;

    /**
     * Virtual type, MySQL server always returned SET flag in definitions and type is VARCHAR.
     */
    public static final short SET = 248;

    /**
     * Note: is also TINYTEXT
     */
    public static final short TINY_BLOB = 249;

    // has no small blob and small text
    /**
     * Note: is also MEDIUMTEXT.
     */
    public static final short MEDIUM_BLOB = 250;

    /**
     * Note: is also LONGTEXT
     */
    public static final short LONG_BLOB = 251;

    /**
     * Note: is also TEXT
     */
    public static final short BLOB = 252;

    public static final short VAR_STRING = 253;

    public static final short STRING = 254;

    public static final short GEOMETRY = 255;

    private DataTypes() {
    }
}
