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

import dev.miku.r2dbc.mysql.codec.CodecContext;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.MySqlType;
import io.r2dbc.spi.ColumnMetadata;
import reactor.util.annotation.NonNull;

/**
 * An abstraction of {@link ColumnMetadata} considers MySQL
 */
public interface MySqlColumnMetadata extends ColumnMetadata {

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlType getType();

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlTypeMetadata getNativeTypeMetadata();

    /**
     * Gets the {@link CharCollation} used for stringification type.  It will not be a binary collation.
     *
     * @param context the codec context for load the default character collation on the server-side.
     * @return the {@link CharCollation}.
     */
    CharCollation getCharCollation(CodecContext context);

    /**
     * Gets the field max size that's defined by the table, the original type is an unsigned int32.
     *
     * @return the field max size.
     */
    long getNativePrecision();

    @NonNull
    @Override
    default Class<?> getJavaType() {
        return getType().getJavaType();
    }
}
