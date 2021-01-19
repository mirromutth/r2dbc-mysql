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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Codec for {@link BigInteger}.
 */
final class BigIntegerCodec extends AbstractClassedCodec<BigInteger> {

    private static final String LONG_MAX_VALUE = Long.toString(Long.MAX_VALUE);

    BigIntegerCodec(ByteBufAllocator allocator) {
        super(allocator, BigInteger.class);
    }

    @Override
    public BigInteger decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary,
        CodecContext context) {
        return binary ? decodeBinary(value, info) : decodeText(value, info);
    }

    @Override
    public boolean canEncode(Object value) {
        // Do not check overflow because it should be ensured by user.
        return value instanceof BigInteger;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new BigIntegerParameter(allocator, (BigInteger) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return TypePredicates.isInt(info.getType());
    }

    private static boolean isGreaterThanMaxValue(String num) {
        int length = num.length();

        if (length != LONG_MAX_VALUE.length()) {
            // If length less than max value length, even it is 999...99, it is also less than max value.
            return length > LONG_MAX_VALUE.length();
        }

        return num.compareTo(LONG_MAX_VALUE) > 0;
    }

    static BigInteger unsignedBigInteger(long negative) {
        byte[] bits = new byte[Long.BYTES + 1];

        bits[0] = 0;
        bits[1] = (byte) (negative >>> 56);
        bits[2] = (byte) (negative >>> 48);
        bits[3] = (byte) (negative >>> 40);
        bits[4] = (byte) (negative >>> 32);
        bits[5] = (byte) (negative >>> 24);
        bits[6] = (byte) (negative >>> 16);
        bits[7] = (byte) (negative >>> 8);
        bits[8] = (byte) negative;

        return new BigInteger(bits);
    }

    private static BigInteger decodeText(ByteBuf value, FieldInformation info) {
        if (info.getType() == DataTypes.BIGINT && (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0) {
            if (value.getByte(value.readerIndex()) == '+') {
                value.skipBytes(1);
            }

            String num = value.toString(StandardCharsets.US_ASCII);

            // Why Java has not BigInteger.parseBigInteger(String)?
            if (isGreaterThanMaxValue(num)) {
                return new BigInteger(num);
            }

            // valueOf can use constant pool.
            return BigInteger.valueOf(parseUnsigned(num));
        }

        return BigInteger.valueOf(LongCodec.parse(value));
    }

    private static BigInteger decodeBinary(ByteBuf value, FieldInformation info) {
        boolean isUnsigned = (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0;

        switch (info.getType()) {
            case DataTypes.BIGINT:
                long v = value.readLongLE();
                if (isUnsigned && v < 0) {
                    return unsignedBigInteger(v);
                }

                return BigInteger.valueOf(v);
            case DataTypes.INT:
                if (isUnsigned) {
                    return BigInteger.valueOf(value.readUnsignedIntLE());
                }

                return BigInteger.valueOf(value.readIntLE());
            case DataTypes.MEDIUMINT:
                // Note: MySQL return 32-bits two's complement for 24-bits integer
                return BigInteger.valueOf(value.readIntLE());
            case DataTypes.SMALLINT:
                if (isUnsigned) {
                    return BigInteger.valueOf(value.readUnsignedShortLE());
                }

                return BigInteger.valueOf(value.readShortLE());
            case DataTypes.YEAR:
                return BigInteger.valueOf(value.readShortLE());
            default: // TINYINT
                if (isUnsigned) {
                    return BigInteger.valueOf(value.readUnsignedByte());
                }

                return BigInteger.valueOf(value.readByte());
        }
    }

    private static long parseUnsigned(String num) {
        long value = 0;
        int size = num.length();

        for (int i = 0; i < size; ++i) {
            value = value * 10L + (num.charAt(i) - '0');
        }

        return value;
    }

    private static class BigIntegerParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final BigInteger value;

        private BigIntegerParameter(ByteBufAllocator allocator, BigInteger value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> BigDecimalCodec.encodeAscii(allocator, value.toString()));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeBigInteger(value));
        }

        @Override
        public short getType() {
            return DataTypes.VARCHAR;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BigIntegerParameter)) {
                return false;
            }
            BigIntegerParameter that = (BigIntegerParameter) o;
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
