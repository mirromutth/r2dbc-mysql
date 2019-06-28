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

import io.github.mirromutth.r2dbc.mysql.codec.FieldInformation;
import io.github.mirromutth.r2dbc.mysql.constant.ColumnDefinitions;
import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import reactor.util.annotation.NonNull;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.require;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ColumnMetadata} for MySQL database.
 * <p>
 * Note: same as parameter metadata in MySQL, but parameter metadata is useless for the SPI of R2DBC.
 */
final class MySqlColumnMetadata implements ColumnMetadata, FieldInformation {

    private final int index;

    private final DataType type;

    private final int nativeType;

    private final String name;

    private final short definitions;

    private final Nullability nullability;

    private final int size;

    private final int decimals;

    private final int collationId;

    private MySqlColumnMetadata(int index, DataType type, int nativeType, String name, short definitions, boolean nonNull, int size, int decimals, int collationId) {
        require(index >= 0, "index must not be a negative integer");
        requireNonNull(type, "type must not be null");
        require(size >= 0, "size must not be a negative integer");
        require(decimals >= 0, "decimals must not be a negative integer");
        requireNonNull(name, "name must not be null");
        require(collationId > 0, "collationId must be a positive integer");

        this.index = index;
        this.type = type;
        this.nativeType = nativeType;
        this.name = name;
        this.definitions = definitions;

        if (nonNull) {
            this.nullability = Nullability.NON_NULL;
        } else {
            this.nullability = Nullability.NULLABLE;
        }

        this.size = size;
        this.decimals = decimals;
        this.collationId = collationId;
    }

    static MySqlColumnMetadata create(int index, DefinitionMetadataMessage message) {
        return new MySqlColumnMetadata(
            index,
            message.getType(),
            message.getNativeType(),
            message.getName(),
            message.getDefinitions(),
            (message.getDefinitions() & ColumnDefinitions.NOT_NULL) != 0,
            message.getSize(),
            message.getDecimals(),
            message.getCollationId()
        );
    }

    int getIndex() {
        return index;
    }

    public DataType getType() {
        return type;
    }

    public short getDefinitions() {
        return definitions;
    }

    @Override
    public Class<?> getJavaType() {
        if (type == null) {
            return null;
        }

        return type.getJavaType(definitions, size, collationId);
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
        return size;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public Integer getScale() {
        // 0x00 means it is an integer or a static string.
        // 0x1f means it is a dynamic string, an original-double or an original-float.
        // 0x00 to 0x51 for the number of digits to right of the decimal point.
        if (type == DataType.DECIMAL || type == DataType.NEW_DECIMAL || type == DataType.DOUBLE || type == DataType.FLOAT) {
            if (decimals >= 0 && decimals <= 0x51) {
                return decimals;
            }
        }

        return null;
    }

    public int getCollationId() {
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

        if (index != that.index) {
            return false;
        }
        if (nativeType != that.nativeType) {
            return false;
        }
        if (definitions != that.definitions) {
            return false;
        }
        if (size != that.size) {
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
        int result = index;
        result = 31 * result + nativeType;
        result = 31 * result + name.hashCode();
        result = 31 * result + (int) definitions;
        result = 31 * result + nullability.hashCode();
        result = 31 * result + size;
        result = 31 * result + decimals;
        result = 31 * result + collationId;
        return result;
    }

    @Override
    public String toString() {
        return "MySqlColumnMetadata{" +
            "index=" + index +
            ", nativeType=" + nativeType +
            ", name='" + name + '\'' +
            ", definitions=" + definitions +
            ", nullability=" + nullability +
            ", size=" + size +
            ", decimals=" + decimals +
            ", collationId=" + collationId +
            '}';
    }
}
