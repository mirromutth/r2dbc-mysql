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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MySqlType}.
 */
class MySqlTypeTest {

    @Test
    void isBinary() {
        for (MySqlType type : MySqlType.values()) {
            if (type.isString() || type.isLob()) {
                assertThat(type.isBinary()).isTrue();
            }

            if (type.isInt() || type.isDecimals()) {
                assertThat(type.isBinary()).isFalse();
            }

            switch (type) {
                case BIT:
                case GEOMETRY:
                case VARBINARY:
                    assertThat(type.isBinary()).isTrue();
                    break;
                default:
                    if (type.isBinary() && !type.isString()) {
                        assertThat(type.name()).matches("[A-Z]*BLOB");
                    }
                    break;
            }
        }
    }

    @Test
    void isInt() {
        for (MySqlType type : MySqlType.values()) {
            if (type == MySqlType.YEAR) {
                assertThat(type.isInt()).isTrue();
            }

            if (type.isBinary() || type.isLob() || type.isDecimals() || type.isString()) {
                assertThat(type.isInt()).isFalse();
            } else if (type.isInt() && type != MySqlType.YEAR) {
                assertThat(type.name()).matches("[A-Z]*INT(_UNSIGNED)?");
            }
        }
    }

    @Test
    void isString() {
        for (MySqlType type : MySqlType.values()) {
            switch (type) {
                case VARCHAR:
                case JSON:
                case ENUM:
                case SET:
                    assertThat(type.isString()).isTrue();
                    break;
                default:
                    if (type.isString()) {
                        assertThat(type.name()).matches("[A-Z]*TEXT");
                    }
                    break;
            }
        }
    }

    @Test
    void isDecimals() {
        for (MySqlType type : MySqlType.values()) {
            if (type.isDecimals()) {
                assertThat(type).isIn(MySqlType.FLOAT, MySqlType.DOUBLE, MySqlType.DECIMAL);
            }
        }
    }

    @Test
    void isLob() {
        for (MySqlType type : MySqlType.values()) {
            if (type.isLob()) {
                assertThat(type.name()).matches("[A-Z]*(TEXT|BLOB)");
            }
        }
    }

    @Test
    void getBinarySize() {
        for (MySqlType type : MySqlType.values()) {
            if (type.isInt()) {
                assertThat(type.getBinarySize()).isBetween(Byte.BYTES, Long.BYTES)
                    .matches(i -> (i & -i) == i, "Should be a power of 2");
            } else if (type.isDecimals()) {
                switch (type) {
                    case FLOAT:
                        assertThat(type.getBinarySize()).isEqualTo(Float.BYTES);
                        break;
                    case DOUBLE:
                        assertThat(type.getBinarySize()).isEqualTo(Double.BYTES);
                        break;
                    default:
                        assertThat(type.getBinarySize()).isEqualTo(0);
                        break;
                }
            } else {
                assertThat(type.getBinarySize()).isEqualTo(0);
            }
        }
    }
}
