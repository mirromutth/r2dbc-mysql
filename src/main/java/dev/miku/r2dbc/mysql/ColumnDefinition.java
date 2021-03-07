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

package dev.miku.r2dbc.mysql;

/**
 * A flag bitmap considers column definitions.
 */
public final class ColumnDefinition {

    private static final short NOT_NULL = 1;

//    public static final short PRIMARY_PART = 1 << 1; // This field is a part of the primary key
//    public static final short UNIQUE_PART = 1 << 2; // This field is a part of a unique key
//    public static final short KEY_PART = 1 << 3; // This field is a part of a normal key
//    public static final short BLOB = 1 << 4;

    private static final short UNSIGNED = 1 << 5;

//    public static final short ZEROFILL = 1 << 6;

    public static final short BINARY = 1 << 7;

    private static final short ENUM = 1 << 8;

//    public static final short AUTO_INCREMENT = 1 << 9;
//    public static final short TIMESTAMP = 1 << 10;

    private static final short SET = 1 << 11; // type is set

//    public static final short NO_DEFAULT = 1 << 12; // column has no default value
//    public static final short ON_UPDATE_NOW = 1 << 13; // field will be set to NOW() in UPDATE statement

    private static final short ALL_USED = NOT_NULL | UNSIGNED | BINARY | ENUM | SET;

    /**
     * The original bitmap of {@link ColumnDefinition this}.
     * <p>
     * MySQL uses 32-bits definition flags, but only returns the lower 16-bits.
     */
    private final short bitmap;

    private ColumnDefinition(short bitmap) {
        this.bitmap = bitmap;
    }

    /**
     * Checks if value is not null.
     *
     * @return if value is not null.
     */
    public boolean isNotNull() {
        return (bitmap & NOT_NULL) != 0;
    }

    /**
     * Checks if value is an unsigned number. e.g. INT UNSIGNED, BIGINT UNSIGNED.
     * <p>
     * Note: IEEE-754 floating types (e.g. DOUBLE/FLOAT) do not supports it in MySQL 8.0+. When creating a
     * column as an unsigned floating type, the server may report a warning.
     *
     * @return if value is an unsigned number.
     */
    public boolean isUnsigned() {
        return (bitmap & UNSIGNED) != 0;
    }

    /**
     * Checks if value is binary data.
     *
     * @return if value is binary data.
     */
    public boolean isBinary() {
        return (bitmap & BINARY) != 0;
    }

    /**
     * Checks if value type is enum.
     *
     * @return if value is an enum.
     */
    public boolean isEnum() {
        return (bitmap & ENUM) != 0;
    }

    /**
     * Checks if value type is set.
     *
     * @return if value is a set.
     */
    public boolean isSet() {
        return (bitmap & SET) != 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnDefinition)) {
            return false;
        }

        ColumnDefinition that = (ColumnDefinition) o;

        return bitmap == that.bitmap;
    }

    @Override
    public int hashCode() {
        return bitmap;
    }

    @Override
    public String toString() {
        return "ColumnDefinition<0x" + Integer.toHexString(bitmap) + '>';
    }

    /**
     * Creates a {@link ColumnDefinition} with column definitions bitmap. It will unset all unknown or useless
     * flags.
     *
     * @param definitions the column definitions bitmap.
     * @return the {@link ColumnDefinition} without unknown or useless flags.
     */
    public static ColumnDefinition of(int definitions) {
        return new ColumnDefinition((short) (definitions & ALL_USED));
    }
}
