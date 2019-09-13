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

package dev.miku.r2dbc.mysql.message.server;

import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.ColumnDefinitions;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.internal.CodecUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Objects;

import static dev.miku.r2dbc.mysql.internal.AssertUtils.require;
import static dev.miku.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * Column or parameter definition metadata message.
 */
public final class DefinitionMetadataMessage implements ServerMessage {

    private static final int MIN_SIZE = 20;

    private final String database;

    private final String tableName;

    private final String originTableName;

    private final String name;

    private final String originName;

    private final int collationId;

    private final long size;

    private final short type;

    private final short definitions;

    private final short decimals;

    private DefinitionMetadataMessage(
        String database, String tableName, String originTableName, String name, String originName,
        int collationId, long size, short type, short definitions, short decimals
    ) {
        require(size >= 0, "size must not be a negative integer");
        require(collationId > 0, "collationId must be a positive integer");

        this.database = requireNonNull(database, "database must not be null");
        this.tableName = requireNonNull(tableName, "tableName must not be null");
        this.originTableName = requireNonNull(originTableName, "originTableName must not be null");
        this.name = requireNonNull(name, "name must not be null");
        this.originName = requireNonNull(originName, "originName must not be null");
        this.collationId = collationId;
        this.size = size;
        this.type = type;
        this.definitions = definitions;
        this.decimals = decimals;
    }

    public String getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }

    public int getCollationId() {
        return collationId;
    }

    public long getSize() {
        return size;
    }

    public short getType() {
        return type;
    }

    public short getDefinitions() {
        return definitions;
    }

    public short getDecimals() {
        return decimals;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DefinitionMetadataMessage)) {
            return false;
        }
        DefinitionMetadataMessage that = (DefinitionMetadataMessage) o;
        return collationId == that.collationId &&
            size == that.size &&
            type == that.type &&
            definitions == that.definitions &&
            decimals == that.decimals &&
            database.equals(that.database) &&
            tableName.equals(that.tableName) &&
            originTableName.equals(that.originTableName) &&
            name.equals(that.name) &&
            originName.equals(that.originName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, tableName, originTableName, name, originName, collationId, size, type, definitions, decimals);
    }

    @Override
    public String toString() {
        return String.format("DefinitionMetadataMessage{database='%s', tableName='%s' (origin:'%s'), name='%s' (origin:'%s'), collationId=%d, size=%d, type=%d, definitions=%x, decimals=%d}",
            database, tableName, originTableName, name, originName, collationId, size, type, definitions, decimals);
    }

    static boolean isLooksLike(ByteBuf buf) {
        int bufSize = buf.readableBytes();

        if (bufSize < MIN_SIZE) {
            return false;
        }

        int readerIndex = buf.readerIndex();
        byte first = buf.getByte(readerIndex);

        if (first != 3) {
            return false;
        }

        // "def".equals(buf.toString(readerIndex + 1, 3, StandardCharsets.US_ASCII))
        return 'd' == buf.getByte(readerIndex + 1) &&
            'e' == buf.getByte(readerIndex + 2) &&
            'f' == buf.getByte(readerIndex + 3);
    }

    static DefinitionMetadataMessage decode(ByteBuf buf, Charset charset) {
        buf.skipBytes(4); // "def" which sized by var integer

        String database = CodecUtils.readVarIntSizedString(buf, charset);
        String tableName = CodecUtils.readVarIntSizedString(buf, charset);
        String originTableName = CodecUtils.readVarIntSizedString(buf, charset);
        String name = CodecUtils.readVarIntSizedString(buf, charset);
        String originName = CodecUtils.readVarIntSizedString(buf, charset);

        CodecUtils.readVarInt(buf); // skip constant 0x0c encoded by var integer

        int collationId = buf.readUnsignedShortLE();
        long size = buf.readUnsignedIntLE();
        short type = buf.readUnsignedByte();
        short definitions = buf.readShortLE();

        if (DataTypes.JSON == type && collationId == CharCollation.BINARY_ID) {
            collationId = CharCollation.clientCharCollation().getId();
        }

        if ((definitions & ColumnDefinitions.SET) != 0) {
            // Maybe need to check if it is a string-like type?
            type = DataTypes.SET;
        } else if ((definitions & ColumnDefinitions.ENUMERABLE) != 0) {
            // Maybe need to check if it is a string-like type?
            type = DataTypes.ENUMERABLE;
        }

        return new DefinitionMetadataMessage(
            database,
            tableName,
            originTableName,
            name,
            originName,
            collationId,
            size,
            type,
            definitions,
            buf.readUnsignedByte()
        );
    }
}
