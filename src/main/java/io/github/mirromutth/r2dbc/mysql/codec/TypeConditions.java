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

package io.github.mirromutth.r2dbc.mysql.codec;

import io.github.mirromutth.r2dbc.mysql.constant.DataType;

import java.util.EnumSet;
import java.util.Set;

/**
 * Type conditions for {@link Codec} implementations.
 */
final class TypeConditions {

    private static final Set<DataType> LOB = EnumSet.of(
        DataType.TINY_BLOB,
        DataType.MEDIUM_BLOB,
        DataType.BLOB,
        DataType.LONG_BLOB
    );

    private static final Set<DataType> INTS = EnumSet.of(
        DataType.TINYINT,
        DataType.YEAR,
        DataType.SMALLINT,
        DataType.MEDIUMINT,
        DataType.INT,
        DataType.BIGINT
    );

    private static final Set<DataType> STRINGS = EnumSet.of(
        DataType.VARCHAR,
        DataType.STRING,
        DataType.VAR_STRING,
        DataType.ENUMERABLE,
        DataType.JSON,
        DataType.SET
    );

    static boolean isLob(DataType type) {
        return LOB.contains(type);
    }

    static boolean isInt(DataType type) {
        return INTS.contains(type);
    }

    static boolean isDecimal(DataType type) {
        return DataType.DECIMAL == type || DataType.NEW_DECIMAL == type;
    }

    static boolean isString(DataType type) {
        return STRINGS.contains(type);
    }

    private TypeConditions() {
    }
}
