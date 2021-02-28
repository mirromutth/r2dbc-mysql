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
 * Codec for {@code int}.
 */
final class IntegerCodec extends AbstractPrimitiveCodec<Integer> {

    IntegerCodec(ByteBufAllocator allocator) {
        super(allocator, Integer.TYPE, Integer.class);
    }

    @Override
    public Integer decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        return binary ? decodeBinary(value, metadata.getType()) : parse(value);
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
    protected boolean doCanDecode(MySqlColumnMetadata metadata) {
        MySqlType type = metadata.getType();

        return type.isInt() && type != MySqlType.BIGINT_UNSIGNED && type != MySqlType.BIGINT &&
            type != MySqlType.INT_UNSIGNED;
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

    private static int decodeBinary(ByteBuf buf, MySqlType type) {
        switch (type) {
            case INT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT:
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

        throw new IllegalStateException("Cannot decode type " + type + " as an Integer");
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
        public MySqlType getType() {
            return MySqlType.INT;
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
