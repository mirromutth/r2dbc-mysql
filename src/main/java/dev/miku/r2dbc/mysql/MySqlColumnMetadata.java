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

import dev.miku.r2dbc.mysql.codec.FieldInformation;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.ColumnDefinitions;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import reactor.util.annotation.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ColumnMetadata} for MySQL database.
 * <p>
 * Note: same as parameter metadata in MySQL, but parameter metadata is useless for the SPI of R2DBC.
 */
final class MySqlColumnMetadata implements ColumnMetadata, FieldInformation {

    private final int index;

    private final short type;

    private final String name;

    private final short definitions;

    private final Nullability nullability;

    private final long size;

    private final int decimals;

    private final int collationId;

    private MySqlColumnMetadata(int index, short type, String name, short definitions, boolean nonNull,
        long size, int decimals, int collationId) {
        require(index >= 0, "index must not be a negative integer");
        require(size >= 0, "size must not be a negative integer");
        require(decimals >= 0, "decimals must not be a negative integer");
        requireNonNull(name, "name must not be null");
        require(collationId > 0, "collationId must be a positive integer");

        this.index = index;
        this.type = type;
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
        short definitions = message.getDefinitions();
        return new MySqlColumnMetadata(index, message.getType(), message.getColumn(), definitions,
            (definitions & ColumnDefinitions.NOT_NULL) != 0, message.getSize(), message.getDecimals(),
            message.getCollationId());
    }

    int getIndex() {
        return index;
    }

    @Override
    public short getType() {
        return type;
    }

    @Override
    public short getDefinitions() {
        return definitions;
    }

    @Override
    public Class<?> getJavaType() {
        switch (type) {
            case DataTypes.DECIMAL:
            case DataTypes.NEW_DECIMAL:
                return BigDecimal.class;
            case DataTypes.TINYINT:
                if ((definitions & ColumnDefinitions.UNSIGNED) != 0) {
                    return Short.class;
                }

                return Byte.class;
            case DataTypes.SMALLINT:
                if ((definitions & ColumnDefinitions.UNSIGNED) != 0) {
                    return Integer.class;
                }

                return Short.class;
            case DataTypes.INT:
                if ((definitions & ColumnDefinitions.UNSIGNED) != 0) {
                    return Long.class;
                }

                return Integer.class;
            case DataTypes.FLOAT:
                return Float.class;
            case DataTypes.DOUBLE:
                return Double.class;
            case DataTypes.TIMESTAMP:
            case DataTypes.DATETIME:
                return LocalDateTime.class;
            case DataTypes.BIGINT:
                if ((definitions & ColumnDefinitions.UNSIGNED) != 0) {
                    return BigInteger.class;
                }

                return Long.class;
            case DataTypes.MEDIUMINT:
                return Integer.class;
            case DataTypes.DATE:
                return LocalDate.class;
            case DataTypes.TIME:
                return LocalTime.class;
            case DataTypes.YEAR:
                // MySQL return 2-bytes in binary result for type YEAR.
                return Short.class;
            case DataTypes.VARCHAR:
            case DataTypes.JSON:
            case DataTypes.ENUMERABLE:
            case DataTypes.VARBINARY:
            case DataTypes.STRING:
            case DataTypes.TINY_BLOB:
            case DataTypes.MEDIUM_BLOB:
            case DataTypes.LONG_BLOB:
            case DataTypes.BLOB:
                if (collationId == CharCollation.BINARY_ID) {
                    return ByteBuffer.class;
                }

                return String.class;
            case DataTypes.BIT:
                return ByteBuffer.class;
            case DataTypes.GEOMETRY:
                // Most of Geometry libraries were using byte[] to encode/decode which based on WKT
                // (includes Extended-WKT) or WKB
                // MySQL using WKB for encoding/decoding, so use byte[] instead of ByteBuffer by default type.
                // It maybe change after R2DBC SPI specify default type for GEOMETRY.
                return byte[].class;
            case DataTypes.SET:
                return String[].class;
            default:
                return null;
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Integer getNativeTypeMetadata() {
        return (int) type;
    }

    @Override
    public Nullability getNullability() {
        return nullability;
    }

    @NonNull
    @Override
    public Integer getPrecision() {
        return (int) size;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public Integer getScale() {
        // 0x00 means it is an integer or a static string.
        // 0x1f means it is a dynamic string, an original-double or an original-float.
        // 0x00 to 0x51 for the number of digits to right of the decimal point.
        if (type == DataTypes.DECIMAL || type == DataTypes.NEW_DECIMAL || type == DataTypes.DOUBLE ||
            type == DataTypes.FLOAT) {
            if (decimals >= 0 && decimals <= 0x51) {
                return decimals;
            }
        }

        return null;
    }

    @Override
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
        MySqlColumnMetadata metadata = (MySqlColumnMetadata) o;
        return index == metadata.index &&
            type == metadata.type &&
            definitions == metadata.definitions &&
            size == metadata.size &&
            decimals == metadata.decimals &&
            collationId == metadata.collationId &&
            name.equals(metadata.name) &&
            nullability == metadata.nullability;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, name, definitions, nullability, size, decimals, collationId);
    }

    @Override
    public String toString() {
        return "MySqlColumnMetadata{index=" + index + ", type=" + type + ", name='" + name +
            "', definitions=" + Integer.toHexString(definitions) + ", nullability=" + nullability +
            ", size=" + size + ", decimals=" + decimals + ", collationId=" + collationId + '}';
    }
}
