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

package io.github.mirromutth.r2dbc.mysql;

import io.github.mirromutth.r2dbc.mysql.constant.ColumnDefinitions;
import io.github.mirromutth.r2dbc.mysql.constant.ColumnType;
import io.github.mirromutth.r2dbc.mysql.message.backend.ColumnMetadataMessage;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNegative;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requirePositive;

/**
 * An implementation of {@link ColumnMetadata} for MySQL database.
 */
final class MySqlColumnMetadata implements ColumnMetadata {

    private final int ordinal;

    @Nullable
    private final ColumnType type;

    private final int nativeType;

    private final String name;

    private final boolean isUnsigned;

    private final Nullability nullability;

    private final int precision;

    private final int decimals;

    private final int collationId;

    private MySqlColumnMetadata(int ordinal, int nativeType, String name, boolean isUnsigned, boolean isNotNull, int precision, int decimals, int collationId) {
        this.ordinal = requireNonNegative(ordinal, "ordinal must not be negative");
        this.nativeType = requireNonNegative(nativeType, "nativeType must not be negative");
        this.name = requireNonNull(name, "name must not be null");
        this.isUnsigned = isUnsigned;

        if (isNotNull) {
            this.nullability = Nullability.NON_NULL;
        } else {
            this.nullability = Nullability.NULLABLE;
        }

        this.precision = requireNonNegative(precision, "precision must not be negative");
        this.decimals = requireNonNegative(decimals, "decimals must not be negative");
        this.type = ColumnType.valueOfNativeType(nativeType);
        this.collationId = requirePositive(collationId, "collationId must be a positive integer");
    }


    static MySqlColumnMetadata create(int ordinal, ColumnMetadataMessage message) {
        return new MySqlColumnMetadata(
            ordinal,
            message.getType(),
            message.getName(),
            (message.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0,
            (message.getDefinitions() & ColumnDefinitions.NOT_NULL) != 0,
            message.getSize(),
            message.getDecimals(),
            message.getCollationId()
        );
    }

    int getOrdinal() {
        return ordinal;
    }

    @Nullable
    public ColumnType getType() {
        return type;
    }

    public boolean isUnsigned() {
        return isUnsigned;
    }

    @Override
    public Class<?> getJavaType() {
        if (type == null) {
            return null;
        }

        return type.getJavaType(isUnsigned, precision);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Integer getNativeTypeMetadata() {
        return nativeType;
    }

    @Override
    public Nullability getNullability() {
        return nullability;
    }

    @NonNull
    @Override
    public Integer getPrecision() {
        return precision;
    }

    @Override
    public Integer getScale() {
        if (decimals >= 0 && decimals <= 0x51) {
            return decimals;
        }

        return null;
    }

    int getCollationId() {
        return collationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlColumnMetadata)) {
            return false;
        }

        MySqlColumnMetadata that = (MySqlColumnMetadata) o;

        if (ordinal != that.ordinal) {
            return false;
        }
        if (nativeType != that.nativeType) {
            return false;
        }
        if (isUnsigned != that.isUnsigned) {
            return false;
        }
        if (precision != that.precision) {
            return false;
        }
        if (decimals != that.decimals) {
            return false;
        }
        if (collationId != that.collationId) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        return nullability == that.nullability;

    }

    @Override
    public int hashCode() {
        int result = ordinal;
        result = 31 * result + nativeType;
        result = 31 * result + name.hashCode();
        result = 31 * result + (isUnsigned ? 1 : 0);
        result = 31 * result + nullability.hashCode();
        result = 31 * result + precision;
        result = 31 * result + decimals;
        result = 31 * result + collationId;
        return result;
    }

    @Override
    public String toString() {
        return "MySqlColumnMetadata{" +
            "ordinal=" + ordinal +
            ", nativeType=" + nativeType +
            ", name=`" + name + '`' +
            ", isUnsigned=" + isUnsigned +
            ", nullability=" + nullability +
            ", precision=" + precision +
            ", decimals=" + decimals +
            ", collationId=" + collationId +
            '}';
    }
}
