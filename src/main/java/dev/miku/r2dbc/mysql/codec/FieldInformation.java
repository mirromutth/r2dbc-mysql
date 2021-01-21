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

package dev.miku.r2dbc.mysql.codec;

import reactor.util.annotation.Nullable;

/**
 * A field information considers column metadata necessary by decode.
 */
public interface FieldInformation {

    /**
     * Get the type identifier of the field data, see {@code DataTypes}.
     *
     * @return the type.
     */
    short getType();

    /**
     * Get the field definition, see {@code ColumnDefinitions}.
     *
     * @return the definition bitmap.
     */
    short getDefinitions();

    /**
     * Get the character collation id.
     *
     * @return the id of {@code CharCollation}.
     */
    int getCollationId();

    /**
     * Get the field max size that's defined by the table, it is an unsigned int32.
     *
     * @return the field max size.
     */
    long getSize();

    /**
     * Get the default java type, {@code null} if this data type is unknown for the driver, never return
     * primitive types or {@link Object}{@code .class}.
     *
     * @return the default java type.
     */
    @Nullable
    Class<?> getJavaType();
}
