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

import dev.miku.r2dbc.mysql.MySqlColumnMetadata;
import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

/**
 * Codec for {@code long}.
 */
final class LongCodec implements PrimitiveCodec<Long> {

    private final ByteBufAllocator allocator;

    LongCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public Long decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        if (binary) {
            return decodeBinary(value, metadata.getType());
        }

        // Note: no check overflow for BIGINT UNSIGNED
        return parse(value);
    }

    @Override
    public boolean canDecode(MySqlColumnMetadata metadata, Class<?> target) {
        MySqlType type = metadata.getType();

        // Here is a special condition. In the application scenario, many times programmers define
        // BIGINT UNSIGNED usually for make sure the ID is not negative, in fact they just use 63-bits.
        // If users force the requirement to convert BIGINT UNSIGNED to Long, should allow this behavior
        // for better performance (BigInteger is obviously slower than long).
        return type.isInt() &&
            (type == MySqlType.BIGINT_UNSIGNED ? Long.class == target : target.isAssignableFrom(Long.class));
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Long;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        long v = (Long) value;

        if ((byte) v == v) {
            return new ByteCodec.ByteParameter(allocator, (byte) v);
        } else if ((short) v == v) {
            return new ShortCodec.ShortParameter(allocator, (short) v);
        } else if ((int) v == v) {
            return new IntegerCodec.IntParameter(allocator, (int) v);
        }

        return new LongParameter(allocator, v);
    }

    @Override
    public boolean canPrimitiveDecode(MySqlColumnMetadata metadata) {
        // Here is a special condition. see `canDecode`.
        return metadata.getType().isInt();
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

    private static long decodeBinary(ByteBuf buf, MySqlType type) {
        switch (type) {
            case BIGINT_UNSIGNED:
            case BIGINT:
                // Note: no check overflow for BIGINT UNSIGNED
                return buf.readLongLE();
            case INT_UNSIGNED:
                return buf.readUnsignedIntLE();
            case INT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT:
                // Note: MySQL return 32-bits two's complement for 24-bits integer
                return buf.readIntLE();
            case SMALLINT_UNSIGNED:
                return buf.readUnsignedShortLE();
            case SMALLINT:
            case YEAR:
                return buf.readShortLE();
            case TINYINT_UNSIGNED:
                return buf.readUnsignedByte();
            case TINYINT:
                return buf.readByte();
        }

        throw new IllegalStateException("Cannot decode type " + type + " as a Long");
    }

    private static final class LongParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final long value;

        private LongParameter(ByteBufAllocator allocator, long value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> allocator.buffer(Long.BYTES).writeLongLE(value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeLong(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.BIGINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LongParameter)) {
                return false;
            }

            LongParameter longValue = (LongParameter) o;

            return value == longValue.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }
}
