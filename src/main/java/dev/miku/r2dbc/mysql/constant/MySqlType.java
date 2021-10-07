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

import dev.miku.r2dbc.mysql.ColumnDefinition;
import io.r2dbc.spi.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;

/**
 * Enumeration of MySQL data types.
 */
public enum MySqlType implements Type {

    /**
     * Unknown type, should check the native type identifier and column definitions.
     */
    UNKNOWN((short) -1, Object.class),

    /**
     * A decimal number type in string form.
     * <p>
     * It should be defined as {@code DECIMAL(M = 10, D = 0)}:
     * <ol><li>{@code M} is the maximum number of digits/precision. The range is 1 to 65</li>
     * <li>{@code D} is the number of scale. The range is 0 to 30, cannot be greater than {@code M}</li></ol>
     */
    DECIMAL(MySqlType.ID_NEW_DECIMAL, BigDecimal.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public boolean isFractional() {
            return true;
        }
    },

    /**
     * A signed 8-bits integer type.
     */
    TINYINT(MySqlType.ID_TINYINT, Byte.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Byte.BYTES;
        }
    },

    /**
     * An unsigned 8-bits integer type.
     */
    TINYINT_UNSIGNED(MySqlType.ID_TINYINT, Short.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Byte.BYTES;
        }
    },

    /**
     * A signed 16-bits integer type.
     */
    SMALLINT(MySqlType.ID_SMALLINT, Short.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Short.BYTES;
        }
    },

    /**
     * An unsigned 16-bits integer type.
     */
    SMALLINT_UNSIGNED(MySqlType.ID_SMALLINT, Integer.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Short.BYTES;
        }
    },

    /**
     * A signed 32-bits integer type.
     */
    INT(MySqlType.ID_INT, Integer.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Integer.BYTES;
        }
    },

    /**
     * An unsigned 32-bits integer type.
     */
    INT_UNSIGNED(MySqlType.ID_INT, Long.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Integer.BYTES;
        }
    },

    /**
     * A IEEE-754 single-precision floating point number type. It cannot be unsigned when the server version
     * is 8.0 or higher. Otherwise, the server will report a warning when defining the column.
     */
    FLOAT(MySqlType.ID_FLOAT, Float.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public boolean isFractional() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Float.BYTES;
        }
    },

    /**
     * A IEEE-754 double-precision floating point number type. It cannot be unsigned when the server version
     * is 8.0 or higher. Otherwise, the server will report a warning when defining the column.
     */
    DOUBLE(MySqlType.ID_DOUBLE, Double.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public boolean isFractional() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Double.BYTES;
        }
    },

    /**
     * A {@code null} type.
     */
    NULL(MySqlType.ID_NULL, Object.class),

    /**
     * A timestamp type, it will automatically synchronize with the server timezone. It still uses string
     * format to transfer the timestamp value.
     */
    TIMESTAMP(MySqlType.ID_TIMESTAMP, ZonedDateTime.class),

    /**
     * A signed 64-bits integer type.
     */
    BIGINT(MySqlType.ID_BIGINT, Long.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Long.BYTES;
        }
    },

    /**
     * An unsigned 64-bits integer type.
     */
    BIGINT_UNSIGNED(MySqlType.ID_BIGINT, BigInteger.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Long.BYTES;
        }
    },

    /**
     * A signed 24-bits integer type.
     */
    MEDIUMINT(MySqlType.ID_MEDIUMINT, Integer.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Integer.BYTES;
        }
    },

    /**
     * An unsigned 24-bits integer type.
     */
    MEDIUMINT_UNSIGNED(MySqlType.ID_MEDIUMINT, Integer.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Integer.BYTES;
        }
    },

    /**
     * A date type. It contains neither time nor timezone.
     */
    DATE(MySqlType.ID_DATE, LocalDate.class),

    /**
     * A time type. It contains neither date nor timezone.
     */
    TIME(MySqlType.ID_TIME, LocalTime.class),

    /**
     * A date time type. It does not contain timezone. It uses string format to transfer the value.
     */
    DATETIME(MySqlType.ID_DATETIME, LocalDateTime.class),

    /**
     * A year type. It contains neither leap year information nor timezone.
     */
    YEAR(MySqlType.ID_YEAR, Short.class) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public int getBinarySize() {
            return Short.BYTES;
        }
    },

    /**
     * A variable-length string type. The MySQL uses custom character collation rules, so the {@code NVARCHAR}
     * is an alias of this type.
     */
    VARCHAR(MySqlType.ID_VAR_STRING, String.class) {
        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A bitmap type. The precision range is 1 to 64.
     */
    BIT(MySqlType.ID_BIT, ByteBuffer.class) {
        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A json type. The maximum length is 4,294,967,295 characters.
     * <p>
     * Note: from MariaDB 10.2.7, it is an alias for {@link #LONGBLOB}.
     */
    JSON(MySqlType.ID_JSON, String.class) {
        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A enumerable string type. It is a virtual type, server will enabled {@code ENUMERABLE} in column
     * definitions and type is as {@link #VARCHAR}.
     */
    ENUM(MySqlType.ID_ENUM, String.class) {
        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A string set type. It is a virtual type, server will enabled {@code SET} in column definitions and type
     * is as {@link #VARCHAR}.
     */
    SET(MySqlType.ID_SET, String[].class) {
        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A binary type. The maximum length 255 bytes.
     */
    TINYBLOB(MySqlType.ID_TINYBLOB, ByteBuffer.class) {
        @Override
        public boolean isLob() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A string type. The maximum length 255 characters.
     */
    TINYTEXT(MySqlType.ID_TINYBLOB, String.class) {
        @Override
        public boolean isLob() {
            return true;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A binary type. The maximum length 16,777,215 bytes.
     */
    MEDIUMBLOB(MySqlType.ID_MEDIUMBLOB, ByteBuffer.class) {
        @Override
        public boolean isLob() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A string type. The maximum length 16,777,215 characters.
     */
    MEDIUMTEXT(MySqlType.ID_MEDIUMBLOB, String.class) {
        @Override
        public boolean isLob() {
            return true;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A binary type. The maximum length is 4,294,967,295 bytes.
     */
    LONGBLOB(MySqlType.ID_LONGBLOB, ByteBuffer.class) {
        @Override
        public boolean isLob() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A string type. The maximum length is 4,294,967,295 characters.
     */
    LONGTEXT(MySqlType.ID_LONGBLOB, String.class) {
        @Override
        public boolean isLob() {
            return true;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A binary type. The maximum length 65,535 bytes.
     */
    BLOB(MySqlType.ID_BLOB, ByteBuffer.class) {
        @Override
        public boolean isLob() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A string type. The maximum length 65,535 characters.
     */
    TEXT(MySqlType.ID_BLOB, String.class) {
        @Override
        public boolean isLob() {
            return true;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A binary type. It uses the same identifier as the varchar.
     */
    VARBINARY(MySqlType.ID_VAR_STRING, ByteBuffer.class) {
        @Override
        public boolean isBinary() {
            return true;
        }
    },

    /**
     * A geometry type. The type can be more specific. Such as POINT, LINESTRING, POLYGON, MULTIPOINT,
     * MULTILINESTRING, MULTIPOLYGON and GEOMETRYCOLLECTION.
     * <p>
     * Explaining this type is a huge task. It's recommended to find official documents to help.
     */
    GEOMETRY(MySqlType.ID_GEOMETRY, byte[].class) {
        @Override
        public boolean isBinary() {
            return true;
        }
    };

    private static final short ID_DECIMAL = 0;

    private static final short ID_TINYINT = 1;

    private static final short ID_SMALLINT = 2;

    private static final short ID_INT = 3;

    private static final short ID_FLOAT = 4;

    private static final short ID_DOUBLE = 5;

    private static final short ID_NULL = 6;

    private static final short ID_TIMESTAMP = 7;

    private static final short ID_BIGINT = 8;

    private static final short ID_MEDIUMINT = 9;

    private static final short ID_DATE = 10;

    private static final short ID_TIME = 11;

    private static final short ID_DATETIME = 12;

    private static final short ID_YEAR = 13;

    // NEW_DATE(14) is internal type of MySQL server, do NOT support this type.

    private static final short ID_VARCHAR = 15;

    private static final short ID_BIT = 16;

    // TIMESTAMP2(17), DATETIME2(18) and TIME2(19) are internal types of MySQL server, do NOT support them.

    private static final short ID_JSON = 245;

    private static final short ID_NEW_DECIMAL = 246;

    private static final short ID_ENUM = 247;

    private static final short ID_SET = 248;

    private static final short ID_TINYBLOB = 249;

    // has no small blob and small text

    private static final short ID_MEDIUMBLOB = 250;

    private static final short ID_LONGBLOB = 251;

    private static final short ID_BLOB = 252;

    private static final short ID_VAR_STRING = 253;

    private static final short ID_STRING = 254;

    private static final short ID_GEOMETRY = 255;

    private final short id;

    private final Class<?> javaType;

    MySqlType(short id, Class<?> javaType) {
        this.id = id;
        this.javaType = javaType;
    }

    public int getId() {
        return id;
    }

    @Override
    public Class<?> getJavaType() {
        return javaType;
    }

    @Override
    public String getName() {
        return name();
    }

    /**
     * Checks if this type is a BLOB or CLOB.
     *
     * @return if it is a BLOB/CLOB type.
     */
    public boolean isLob() {
        return false;
    }

    /**
     * Checks if this type can be decoded as a string without loss of precision and without ambiguity in both
     * of text and binary protocol. e.g. JSON, VARCHAR, SET, etc.
     * <p>
     * Counterexample:
     * <ul><li>INT/DOUBLE/DATETIME uses different encodings in text protocol and binary protocol.</li>
     * <li>BIT/VARBINARY/BLOB cannot be decoded as a string.</li></ul>
     *
     * @return if it is a string type.
     */
    public boolean isString() {
        return false;
    }

    /**
     * Checks if this type is a numeric type. The {@link #YEAR} is an 16-bits integer.
     *
     * @return if it is a numeric type.
     */
    public boolean isNumeric() {
        return false;
    }

    /**
     * Checks if this type can be decoded as a fractional number.  This means that it may have the scale of
     * the column.
     *
     * @return if it is a fractional type.
     */
    public boolean isFractional() {
        return false;
    }

    /**
     * Checks if this type can be decoded as a binary buffer, all string types should be {@code true}.
     *
     * @return if it is a binary type.
     */
    public boolean isBinary() {
        return false;
    }

    /**
     * Get the fixed byte size of the data type in the binary protocol, otherwise {@literal 0} means that
     * there is no fixed size.
     *
     * @return the fixed size in binary protocol.
     */
    public int getBinarySize() {
        return 0;
    }

    public static MySqlType of(int id, ColumnDefinition definition) {
        // Maybe need to check if it is a string-like type?
        if (definition.isSet()) {
            return SET;
        } else if (definition.isEnum()) {
            return ENUM;
        }

        switch (id) {
            case ID_DECIMAL:
            case ID_NEW_DECIMAL:
                return DECIMAL;
            case ID_TINYINT:
                return definition.isUnsigned() ? TINYINT_UNSIGNED : TINYINT;
            case ID_SMALLINT:
                return definition.isUnsigned() ? SMALLINT_UNSIGNED : SMALLINT;
            case ID_INT:
                return definition.isUnsigned() ? INT_UNSIGNED : INT;
            case ID_FLOAT:
                return FLOAT;
            case ID_DOUBLE:
                return DOUBLE;
            case ID_NULL:
                return NULL;
            case ID_TIMESTAMP:
                return TIMESTAMP;
            case ID_BIGINT:
                return definition.isUnsigned() ? BIGINT_UNSIGNED : BIGINT;
            case ID_MEDIUMINT:
                return definition.isUnsigned() ? MEDIUMINT_UNSIGNED : MEDIUMINT;
            case ID_DATE:
                return DATE;
            case ID_TIME:
                return TIME;
            case ID_DATETIME:
                return DATETIME;
            case ID_YEAR:
                return YEAR;
            case ID_VARCHAR:
            case ID_VAR_STRING:
            case ID_STRING:
                return definition.isBinary() ? VARBINARY : VARCHAR;
            case ID_BIT:
                return BIT;
            case ID_JSON:
                return JSON;
            case ID_ENUM:
                return ENUM;
            case ID_SET:
                return SET;
            case ID_TINYBLOB:
                return definition.isBinary() ? TINYBLOB : TINYTEXT;
            case ID_MEDIUMBLOB:
                return definition.isBinary() ? MEDIUMBLOB : MEDIUMTEXT;
            case ID_LONGBLOB:
                return definition.isBinary() ? LONGBLOB : LONGTEXT;
            case ID_BLOB:
                return definition.isBinary() ? BLOB : TEXT;
            case ID_GEOMETRY:
                // Most of Geometry libraries were using byte[] to encode/decode which based on WKT
                // (includes Extended-WKT) or WKB
                // MySQL using WKB for encoding/decoding, so use byte[] instead of ByteBuffer by default type.
                // It maybe change after R2DBC SPI specify default type for GEOMETRY.
                return GEOMETRY;
            default:
                return UNKNOWN;
        }
    }
}
