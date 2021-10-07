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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Codec for {@link BigInteger}.
 */
final class BigIntegerCodec extends AbstractClassedCodec<BigInteger> {

    BigIntegerCodec(ByteBufAllocator allocator) {
        super(allocator, BigInteger.class);
    }

    @Override
    public BigInteger decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        MySqlType type = metadata.getType();

        if (binary) {
            return decodeBinary(value, type);
        }

        switch (type) {
            case FLOAT:
                return BigDecimal.valueOf(Float.parseFloat(value.toString(StandardCharsets.US_ASCII)))
                    .toBigInteger();
            case DOUBLE:
                return BigDecimal.valueOf(Double.parseDouble(value.toString(StandardCharsets.US_ASCII)))
                    .toBigInteger();
            case DECIMAL:
                return decimalBigInteger(value);
            case BIGINT_UNSIGNED:
                if (value.getByte(value.readerIndex()) == '+') {
                    value.skipBytes(1);
                }

                // Why Java has not BigInteger.parseBigInteger(String)?
                String num = value.toString(StandardCharsets.US_ASCII);

                if (CodecUtils.isGreaterThanLongMax(num)) {
                    return new BigInteger(num);
                }

                return BigInteger.valueOf(CodecUtils.parsePositive(num));
            default:
                return BigInteger.valueOf(CodecUtils.parseLong(value));
        }
    }

    @Override
    public boolean canEncode(Object value) {
        // Do not check overflow because it should be ensured by user.
        return value instanceof BigInteger;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        BigInteger i = (BigInteger) value;

        if (i.bitLength() < Long.SIZE) {
            return LongCodec.encodeLong(allocator, i.longValue());
        }

        return new BigIntegerParameter(allocator, (BigInteger) value);
    }

    @Override
    protected boolean doCanDecode(MySqlColumnMetadata metadata) {
        return metadata.getType().isNumeric();
    }

    private static BigInteger decodeBinary(ByteBuf buf, MySqlType type) {
        switch (type) {
            case BIGINT_UNSIGNED:
                long v = buf.readLongLE();

                if (v < 0) {
                    return CodecUtils.unsignedBigInteger(v);
                }

                return BigInteger.valueOf(v);
            case BIGINT:
                return BigInteger.valueOf(buf.readLongLE());
            case INT_UNSIGNED:
                return BigInteger.valueOf(buf.readUnsignedIntLE());
            case INT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT:
                // Note: MySQL return 32-bits two's complement for 24-bits integer
                return BigInteger.valueOf(buf.readIntLE());
            case SMALLINT_UNSIGNED:
                return BigInteger.valueOf(buf.readUnsignedShortLE());
            case SMALLINT:
            case YEAR:
                return BigInteger.valueOf(buf.readShortLE());
            case TINYINT_UNSIGNED:
                return BigInteger.valueOf(buf.readUnsignedByte());
            case TINYINT:
                return BigInteger.valueOf(buf.readByte());
            case DECIMAL:
                return decimalBigInteger(buf);
            case FLOAT:
                return BigDecimal.valueOf(buf.readFloatLE()).toBigInteger();
            case DOUBLE:
                return BigDecimal.valueOf(buf.readDoubleLE()).toBigInteger();
        }

        throw new IllegalStateException("Cannot decode type " + type + " as a BigInteger");
    }

    private static BigInteger decimalBigInteger(ByteBuf buf) {
        return new BigDecimal(buf.toString(StandardCharsets.US_ASCII)).toBigInteger();
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
            return Mono.fromSupplier(() -> CodecUtils.encodeAscii(allocator, value.toString()));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeBigInteger(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.VARCHAR;
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
