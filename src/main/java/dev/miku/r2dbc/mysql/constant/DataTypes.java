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
 * MySQL column/parameter data types.
 */
public final class DataTypes {

    /**
     * A decimal number type in string form.
     * <p>
     * It should be defined as {@code DECIMAL(M = 10, D = 0)}:
     * <ol><li>{@code M} is the maximum number of digits/precision. The range is 1 to 65</li>
     * <li>{@code D} is the number of scale. The range is 0 to 30, cannot be greater than {@code M}</li></ol>
     */
    public static final short DECIMAL = 0;

    /**
     * An 8-bits integer type. It can be unsigned.
     */
    public static final short TINYINT = 1;

    /**
     * A 16-bits integer type. It can be unsigned.
     */
    public static final short SMALLINT = 2;

    /**
     * A 32-bits integer type. It can be unsigned.
     */
    public static final short INT = 3;

    /**
     * A IEEE-754 single-precision floating point number type. It cannot be unsigned when the server version
     * is 8.0 or higher. Otherwise, the server will report a warning when defining the column.
     */
    public static final short FLOAT = 4;

    /**
     * A IEEE-754 double-precision floating point number type. It cannot be unsigned when the server version
     * is 8.0 or higher. Otherwise, the server will report a warning when defining the column.
     */
    public static final short DOUBLE = 5;

    /**
     * A {@code null} type.
     */
    public static final short NULL = 6;

    /**
     * A timestamp type, it will automatically synchronize with the server timezone. It still uses string
     * format to transfer the timestamp value.
     */
    public static final short TIMESTAMP = 7;

    /**
     * A 64-bits integer type. It can be unsigned.
     */
    public static final short BIGINT = 8;

    /**
     * A 24-bits integer type. It can be unsigned.
     */
    public static final short MEDIUMINT = 9;

    /**
     * A date type. It contains neither time nor timezone.
     */
    public static final short DATE = 10;

    /**
     * A time type. It contains neither date nor timezone.
     */
    public static final short TIME = 11;

    /**
     * A date time type. It does not contain timezone. It uses string format to transfer the value.
     */
    public static final short DATETIME = 12;

    /**
     * A year type. It contains neither leap year information nor timezone.
     */
    public static final short YEAR = 13;

    // NEW_DATE (14) is internal type of MySQL server, do NOT support this type.

    /**
     * A variable-length string type. The MySQL uses custom character collation rules, so the {@code NVARCHAR}
     * is an alias of this type.
     */
    public static final short VARCHAR = 15;

    /**
     * A bitmap type. The precision range is 1 to 64.
     */
    public static final short BIT = 16;

    // TIMESTAMP2(17), DATETIME2 (18) and TIME2 (19) are internal types of MySQL server, do NOT support them.

    /**
     * A json type. The maximum length is 4,294,967,295 characters.
     * <p>
     * Note: from MariaDB 10.2.7, {@code JSON} is an alias for {@link #LONG_BLOB}.
     */
    public static final short JSON = 245;

    /**
     * A decimal number type in string form.
     */
    public static final short NEW_DECIMAL = 246;

    /**
     * A enumerable string type. It is a virtual type, server will enabled {@code ENUMERABLE} in column
     * definitions and type is as {@link #VARCHAR}.
     */
    public static final short ENUMERABLE = 247;

    /**
     * A string set type. It is a virtual type, server will enabled {@code SET} in column definitions and type
     * is as {@link #VARCHAR}.
     */
    public static final short SET = 248;

    /**
     * A binary or string type. The maximum length 255 characters/bytes. If character collation is defined as
     * {@code BINARY}, the field will be binary data and the precision is byte width. Otherwise, the field
     * will be string data, and the precision is the string length.
     * <p>
     * Note: is also {@code TINYTEXT}.
     */
    public static final short TINY_BLOB = 249;

    // has no small blob and small text

    /**
     * A binary or string type. The maximum length 16,777,215 characters/bytes. If character collation is
     * defined as {@code BINARY}, the field will be binary data and the precision is byte width. Otherwise,
     * the field will be string data, and the precision is the string length.
     * <p>
     * Note: is also MEDIUMTEXT.
     */
    public static final short MEDIUM_BLOB = 250;

    /**
     * A binary or string type. The maximum length is 4,294,967,295 characters/bytes. If character collation
     * is defined as {@code BINARY}, the field will be binary data and the precision is byte width. Otherwise,
     * the field will be string data, and the precision is the string length.
     * <p>
     * Note: is also LONGTEXT
     */
    public static final short LONG_BLOB = 251;

    /**
     * A binary or string type. The maximum length 65,535 characters/bytes. If character collation is defined
     * as {@code BINARY}, the field will be binary data and the precision is byte width. Otherwise, the field
     * will be string data, and the precision is the string length.
     * <p>
     * Note: is also TEXT
     */
    public static final short BLOB = 252;

    /**
     * A binary type.
     * <p>
     * i.e. VAR STRING in MySQL documentation, but it is binary type.
     */
    public static final short VARBINARY = 253;

    /**
     * A string type, usually use {@link #VARCHAR} instead.
     */
    public static final short STRING = 254;

    /**
     * A geometry type. The type can be more specific. Such as POINT, LINESTRING, POLYGON, MULTIPOINT,
     * MULTILINESTRING, MULTIPOLYGON and GEOMETRYCOLLECTION.
     * <p>
     * Explaining this type is a huge task. It's recommended to find official documents to help.
     */
    public static final short GEOMETRY = 255;

    private DataTypes() { }
}
