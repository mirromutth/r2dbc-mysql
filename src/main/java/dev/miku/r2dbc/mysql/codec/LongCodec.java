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

import dev.miku.r2dbc.mysql.constant.ColumnDefinitions;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

/**
 * Codec for {@link long}.
 */
final class LongCodec implements PrimitiveCodec<Long> {

    static final LongCodec INSTANCE = new LongCodec();

    private LongCodec() {
    }

    @Override
    public Long decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        if (binary) {
            boolean isUnsigned = (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0;
            return decodeBinary(value, info.getType(), isUnsigned);
        } else {
            // Note: no check overflow for BIGINT UNSIGNED
            return parse(value);
        }
    }

    @Override
    public boolean canDecode(boolean massive, FieldInformation info, Class<?> target) {
        short type = info.getType();

        if (!TypePredicates.isInt(type) || massive) {
            return false;
        }

        // Here is a special condition. In the application scenario, many times programmers define
        // BIGINT UNSIGNED usually for make sure the ID is not negative, in fact they just use 63-bits.
        // If users force the requirement to convert BIGINT UNSIGNED to Long, should allow this behavior
        // for better performance (BigInteger is obviously slower than long).
        if (DataTypes.BIGINT == type && (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0) {
            return Long.class == target;
        } else {
            return target.isAssignableFrom(Long.class);
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Long;
    }

    @Override
    public ParameterValue encode(Object value, CodecContext context) {
        long v = (Long) value;

        if ((byte) v == v) {
            return new ByteCodec.ByteValue((byte) v);
        }

        if ((short) v == v) {
            return new ShortCodec.ShortValue((short) v);
        }

        if ((int) v == v) {
            return new IntegerCodec.IntValue((int) v);
        }

        return new LongValue(v);
    }

    @Override
    public boolean canPrimitiveDecode(FieldInformation info) {
        // Here is a special condition. see `canDecode`.
        return TypePredicates.isInt(info.getType());
    }

    @Override
    public Class<Long> getPrimitiveClass() {
        return Long.TYPE;
    }

    /**
     * Fast parse a negotiable integer from {@link ByteBuf} without copy.
     *
     * @param buf a {@link ByteBuf} include an integer that maybe has sign.
     * @return an integer from {@code buf}.
     */
    static long parse(ByteBuf buf) {
        byte first = buf.readByte();
        boolean isNegative;
        long value;

        if (first == '-') {
            isNegative = true;
            value = 0L;
        } else if (first >= '0' && first <= '9') {
            isNegative = false;
            value = (long) first - '0';
        } else {
            // Must be '+'.
            isNegative = false;
            value = 0L;
        }

        while (buf.isReadable()) {
            value = value * 10L + (buf.readByte() - '0');
        }

        return isNegative ? -value : value;
    }

    private static long decodeBinary(ByteBuf buf, short type, boolean isUnsigned) {
        switch (type) {
            case DataTypes.BIGINT:
                // Note: no check overflow for BIGINT UNSIGNED
                return buf.readLongLE();
            case DataTypes.INT:
                if (isUnsigned) {
                    return buf.readUnsignedIntLE();
                } else {
                    return buf.readIntLE();
                }
            case DataTypes.MEDIUMINT:
                // Note: MySQL return 32-bits two's complement for 24-bits integer
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

    private static final class LongValue extends AbstractParameterValue {

        private final long value;

        private LongValue(long value) {
            this.value = value;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeLong(value));
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.fromRunnable(() -> builder.append(value));
        }

        @Override
        public short getType() {
            return DataTypes.BIGINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LongValue)) {
                return false;
            }

            LongValue longValue = (LongValue) o;

            return value == longValue.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }
}
