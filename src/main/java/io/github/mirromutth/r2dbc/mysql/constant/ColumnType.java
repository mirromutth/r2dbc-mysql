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

import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL column data types and define main java type.
 */
public enum ColumnType {

    DECIMAL(0) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return BigDecimal.class;
        }
    },
    TINYINT(1) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            if (isUnsigned) {
                return short.class;
            } else {
                return byte.class;
            }
        }
    },
    SMALLINT(2) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            if (isUnsigned) {
                return int.class;
            } else {
                return short.class;
            }
        }
    },
    INT(3) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            if (isUnsigned) {
                return long.class;
            } else {
                return int.class;
            }
        }
    },
    FLOAT(4) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return float.class;
        }
    },
    DOUBLE(5) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return double.class;
        }
    },
    NULL(6), // Maybe bug if value is not null?
    TIMESTAMP(7) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return Instant.class;
        }
    },
    BIGINT(8) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            if (isUnsigned) {
                return BigInteger.class;
            } else {
                return long.class;
            }
        }
    },
    MEDIUMINT(9) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return int.class;
        }
    },
    DATE(10) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return LocalDate.class;
        }
    },
    TIME(11) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return LocalTime.class;
        }
    },
    DATETIME(12) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return LocalDateTime.class;
        }
    },
    YEAR(13) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return Year.class;
        }
    },
    // NEW_DATE (14) is internal type of MySQL server, do NOT support this type.
    VARCHAR(15) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return String.class;
        }
    },
    BIT(16) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            // maybe use BitSet?
            return byte[].class;
        }
    },
    TIMESTAMP2(17) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return Instant.class;
        }
    },
    // DATETIME2 (18) and TIME2 (19) are internal types, do NOT support them.
    JSON(245),
    NEW_DECIMAL(246) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return BigDecimal.class;
        }
    },
    ENUMERABLE(247) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return String.class;
        }
    },
    SET(248) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return String[].class;
        }
    },
    TINY_BLOB(249) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return byte[].class;
        }
    },
    MEDIUM_BLOB(250) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return byte[].class;
        }
    },
    LONG_BLOB(251) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return byte[].class;
        }
    },
    BLOB(252) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return byte[].class;
        }
    },
    VAR_STRING(253) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return String.class;
        }
    },
    STRING(254) {
        @Override
        public Class<?> getJavaType(boolean isUnsigned, int precision) {
            return String.class;
        }
    },
    GEOMETRY(255); // maybe not support or just return a String

    private static final Map<Integer, ColumnType> NATIVE_TYPE_KEYED = buildMap();

    private final int nativeType;

    ColumnType(int nativeType) {
        this.nativeType = nativeType;
    }

    public int getType() {
        return nativeType;
    }

    @Nullable
    public Class<?> getJavaType(boolean isUnsigned, int precision) {
        return null;
    }

    @Nullable
    public static ColumnType valueOfNativeType(int type) {
        return NATIVE_TYPE_KEYED.get(type);
    }

    private static Map<Integer, ColumnType> buildMap() {
        ColumnType[] types = ColumnType.values();
        // ceil(size / 0.75) = ceil(size / 3 * 4) = ceil(size / 3) * 4 = floor((size + 3 - 1) / 3) * 4
        Map<Integer, ColumnType> map = new HashMap<>(((types.length + 2) / 3) * 4, 0.75f);

        for (ColumnType type : types) {
            map.put(type.nativeType, type);
        }

        return map;
    }
}
