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

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL column/parameter data types and define main java type.
 */
public enum DataType {

    /**
     * Virtual type for unknown column type.
     */
    UNKNOWN(-1),
    DECIMAL(0) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return BigDecimal.class;
        }
    },
    TINYINT(1) {

        @Override
        public int getFixedBinaryBytes() {
            return Byte.BYTES;
        }

        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            if ((definitions & ColumnDefinitions.UNSIGNED) != 0) {
                return Short.TYPE;
            } else {
                return Byte.TYPE;
            }
        }
    },
    SMALLINT(2) {

        @Override
        public int getFixedBinaryBytes() {
            return Short.BYTES;
        }

        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            if ((definitions & ColumnDefinitions.UNSIGNED) != 0) {
                return Integer.TYPE;
            } else {
                return Short.TYPE;
            }
        }
    },
    INT(3) {

        @Override
        public int getFixedBinaryBytes() {
            return Integer.BYTES;
        }

        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            if ((definitions & ColumnDefinitions.UNSIGNED) != 0) {
                return Long.TYPE;
            } else {
                return Integer.TYPE;
            }
        }
    },
    FLOAT(4) {

        @Override
        public int getFixedBinaryBytes() {
            return Float.BYTES;
        }

        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return Float.TYPE;
        }
    },
    DOUBLE(5) {

        @Override
        public int getFixedBinaryBytes() {
            return Double.BYTES;
        }

        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return Double.TYPE;
        }
    },
    NULL(6),
    TIMESTAMP(7) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return LocalDateTime.class;
        }
    },
    BIGINT(8) {

        @Override
        public int getFixedBinaryBytes() {
            return Long.BYTES;
        }

        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            if ((definitions & ColumnDefinitions.UNSIGNED) != 0) {
                return BigInteger.class;
            } else {
                return Long.TYPE;
            }
        }
    },
    MEDIUMINT(9) {

        @Override
        public int getFixedBinaryBytes() {
            return Integer.BYTES;
        }

        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return Integer.TYPE;
        }
    },
    DATE(10) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return LocalDate.class;
        }
    },
    TIME(11) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return LocalTime.class;
        }
    },
    DATETIME(12) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return LocalDateTime.class;
        }
    },
    YEAR(13) {

        @Override
        public int getFixedBinaryBytes() {
            return Short.BYTES;
        }

        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            // MySQL return 2-bytes in binary result for type YEAR.
            return Short.TYPE;
        }
    },
    // NEW_DATE (14) is internal type of MySQL server, do NOT support this type.
    VARCHAR(15) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return String.class;
        }
    },
    BIT(16) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return BitSet.class;
        }
    },
    TIMESTAMP2(17) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return LocalDateTime.class;
        }
    },
    // DATETIME2 (18) and TIME2 (19) are internal types, do NOT support them.
    JSON(245) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return String.class;
        }
    },
    NEW_DECIMAL(246) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return BigDecimal.class;
        }
    },
    /**
     * Virtual type, MySQL server always returned ENUMERABLE flag in definitions and type is VARCHAR.
     */
    ENUMERABLE(247) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return String.class;
        }
    },
    /**
     * Virtual type, MySQL server always returned SET flag in definitions and type is VARCHAR.
     */
    SET(248) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return String[].class;
        }
    },
    /**
     * Note: is also TINYTEXT
     */
    TINY_BLOB(249) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            if (collationId == CharCollation.BINARY_ID) {
                return Blob.class;
            } else {
                return Clob.class;
            }
        }
    },
    /**
     * Note: is also MEDIUMTEXT, has no small blob and small text.
     */
    MEDIUM_BLOB(250) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            if (collationId == CharCollation.BINARY_ID) {
                return Blob.class;
            } else {
                return Clob.class;
            }
        }
    },
    /**
     * Note: is also LONGTEXT
     */
    LONG_BLOB(251) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            if (collationId == CharCollation.BINARY_ID) {
                return Blob.class;
            } else {
                return Clob.class;
            }
        }
    },
    /**
     * Note: is also TEXT
     */
    BLOB(252) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            if (collationId == CharCollation.BINARY_ID) {
                return Blob.class;
            } else {
                return Clob.class;
            }
        }
    },
    VAR_STRING(253) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return String.class;
        }
    },
    STRING(254) {
        @Override
        public Class<?> getJavaType(short definitions, int size, int collationId) {
            return String.class;
        }
    },
    GEOMETRY(255); // Not support for now

    private static final Map<Integer, DataType> NATIVE_TYPE_KEYED = buildMap();

    private final int nativeType;

    DataType(int nativeType) {
        this.nativeType = nativeType;
    }

    public int getType() {
        return nativeType;
    }

    @Nullable
    public Class<?> getJavaType(short definitions, int size, int collationId) {
        return null;
    }

    /**
     * @return {@literal 0} means field is var integer sized in binary result.
     */
    public int getFixedBinaryBytes() {
        return 0;
    }

    public static DataType valueOfNativeType(int type, short definitions) {
        if ((definitions & ColumnDefinitions.ENUMERABLE) != 0) {
            return DataType.ENUMERABLE;
        }

        if ((definitions & ColumnDefinitions.SET) != 0) {
            return DataType.SET;
        }

        DataType result = NATIVE_TYPE_KEYED.get(type);

        if (result == null) {
            return UNKNOWN;
        }

        return result;
    }

    private static Map<Integer, DataType> buildMap() {
        DataType[] types = DataType.values();
        // ceil(size / 0.75) = ceil((size * 4) / 3) = floor((size * 4 + 3 - 1) / 3)
        Map<Integer, DataType> map = new HashMap<>(((types.length << 2) + 2) / 3, 0.75f);

        for (DataType type : types) {
            if (type != UNKNOWN && type != ENUMERABLE && type != SET) {
                map.put(type.nativeType, type);
            }
        }

        return map;
    }
}
