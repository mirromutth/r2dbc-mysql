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

import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.ColumnDefinitions;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

/**
 * Codec for {@link int}.
 */
final class IntegerCodec extends AbstractPrimitiveCodec<Integer> {

    IntegerCodec(ByteBufAllocator allocator) {
        super(allocator, Integer.TYPE, Integer.class);
    }

    @Override
    public Integer decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        if (binary) {
            boolean isUnsigned = (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0;
            return decodeBinary(value, info.getType(), isUnsigned);
        } else {
            return parse(value);
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Integer;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        int v = (Integer) value;

        if ((byte) v == v) {
            return new ByteCodec.ByteParameter(allocator, (byte) v);
        }

        if ((short) v == v) {
            return new ShortCodec.ShortParameter(allocator, (short) v);
        }

        return new IntParameter(allocator, v);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        short type = info.getType();
        return isLowerInt(type) || (DataTypes.INT == type && (info.getDefinitions() & ColumnDefinitions.UNSIGNED) == 0);
    }

    /**
     * Fast parse a negotiable integer from {@link ByteBuf} without copy.
     *
     * @param buf a {@link ByteBuf} include an integer that maybe has sign.
     * @return an integer from {@code buf}.
     */
    static int parse(ByteBuf buf) {
        byte first = buf.readByte();
        boolean isNegative;
        int value;

        if (first == '-') {
            isNegative = true;
            value = 0;
        } else if (first >= '0' && first <= '9') {
            isNegative = false;
            value = first - '0';
        } else {
            // Must be '+'.
            isNegative = false;
            value = 0;
        }

        while (buf.isReadable()) {
            value = value * 10 + (buf.readByte() - '0');
        }

        return isNegative ? -value : value;
    }

    private static boolean isLowerInt(short type) {
        return DataTypes.TINYINT == type ||
            DataTypes.YEAR == type ||
            DataTypes.SMALLINT == type ||
            DataTypes.MEDIUMINT == type;
    }

    private static int decodeBinary(ByteBuf buf, short type, boolean isUnsigned) {
        switch (type) {
            case DataTypes.INT: // Already check overflow in `doCanDecode`
            case DataTypes.MEDIUMINT:
                return buf.readIntLE();
            case DataTypes.SMALLINT:
                if (isUnsigned) {
                    return buf.readUnsignedShortLE();
                } else {
                    return buf.readShortLE();
                }
            case DataTypes.YEAR:
                return buf.readShortLE();
            default: // TINYINT
                if (isUnsigned) {
                    return buf.readUnsignedByte();
                } else {
                    return buf.readByte();
                }
        }
    }

    static final class IntParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final int value;

        IntParameter(ByteBufAllocator allocator, int value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> allocator.buffer(Integer.BYTES).writeIntLE(value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeInt(value));
        }

        @Override
        public short getType() {
            return DataTypes.INT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IntParameter)) {
                return false;
            }

            IntParameter intValue = (IntParameter) o;

            return value == intValue.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
