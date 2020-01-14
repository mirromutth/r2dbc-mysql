/*
 * Copyright 2018-2020 the original author or authors.
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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.constant.DataTypes;

/**
 * Type predicates for {@link Codec} implementations.
 */
final class TypePredicates {

    static boolean isLob(short type) {
        return DataTypes.TINY_BLOB == type ||
            DataTypes.MEDIUM_BLOB == type ||
            DataTypes.BLOB == type ||
            DataTypes.LONG_BLOB == type;
    }

    static boolean isInt(short type) {
        return DataTypes.TINYINT == type ||
            DataTypes.YEAR == type ||
            DataTypes.SMALLINT == type ||
            DataTypes.MEDIUMINT == type ||
            DataTypes.INT == type ||
            DataTypes.BIGINT == type;
    }

    static boolean isDecimal(short type) {
        return DataTypes.DECIMAL == type || DataTypes.NEW_DECIMAL == type;
    }

    static boolean isString(short type) {
        return DataTypes.VARCHAR == type ||
            DataTypes.STRING == type ||
            DataTypes.VARBINARY == type ||
            DataTypes.ENUMERABLE == type ||
            DataTypes.JSON == type ||
            DataTypes.SET == type;
    }

    static boolean isBinary(short type) {
        return DataTypes.BIT == type || DataTypes.GEOMETRY == type || isString(type) || isLob(type);
    }

    private TypePredicates() {
    }
}
