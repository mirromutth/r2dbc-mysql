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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static dev.miku.r2dbc.mysql.constant.MySqlType.BIGINT;
import static dev.miku.r2dbc.mysql.constant.MySqlType.BIGINT_UNSIGNED;
import static dev.miku.r2dbc.mysql.constant.MySqlType.DECIMAL;
import static dev.miku.r2dbc.mysql.constant.MySqlType.DOUBLE;
import static dev.miku.r2dbc.mysql.constant.MySqlType.FLOAT;
import static dev.miku.r2dbc.mysql.constant.MySqlType.INT;
import static dev.miku.r2dbc.mysql.constant.MySqlType.INT_UNSIGNED;
import static dev.miku.r2dbc.mysql.constant.MySqlType.MEDIUMINT;
import static dev.miku.r2dbc.mysql.constant.MySqlType.MEDIUMINT_UNSIGNED;
import static dev.miku.r2dbc.mysql.constant.MySqlType.SMALLINT;
import static dev.miku.r2dbc.mysql.constant.MySqlType.SMALLINT_UNSIGNED;
import static dev.miku.r2dbc.mysql.constant.MySqlType.TINYINT;
import static dev.miku.r2dbc.mysql.constant.MySqlType.TINYINT_UNSIGNED;
import static dev.miku.r2dbc.mysql.constant.MySqlType.YEAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BigIntegerCodec}.
 */
class BigIntegerCodecTest extends NumericCodecTestSupport<BigInteger> {

    private final BigInteger[] integers = {
        BigInteger.ZERO,
        BigInteger.ONE,
        BigInteger.TEN,
        BigInteger.valueOf(-1),
        BigInteger.valueOf(-2021),
        BigInteger.valueOf(Long.MAX_VALUE),
        new BigInteger(Long.toUnsignedString(Long.MIN_VALUE)), // Max Int64 + 1
        BigInteger.valueOf(Long.MIN_VALUE),
        BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
        new BigInteger(Long.toUnsignedString(-1)), // Max Uint64
        BigInteger.valueOf(Long.MAX_VALUE).pow(7), // Over than max Uint64
    };

    @Override
    public BigIntegerCodec getCodec(ByteBufAllocator allocator) {
        return new BigIntegerCodec(allocator);
    }

    @Override
    public BigInteger[] originParameters() {
        assertThat(new BigInteger(Long.toUnsignedString(Long.MIN_VALUE)))
            .isEqualTo(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        return integers;
    }

    @Override
    public Object[] stringifyParameters() {
        return integers;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(integers).map(this::convert).toArray(ByteBuf[]::new);
    }

    @Override
    public Decoding[] decoding(boolean binary, Charset charset) {
        return decimals().flatMap(it -> {
            List<Decoding> d = new ArrayList<>();
            BigInteger res = it.toBigInteger();

            d.add(new Decoding(encodeAscii(it.toString()), res, DECIMAL));

            float fv = it.floatValue();

            if (Float.isFinite(fv) && BigDecimal.valueOf(fv).toBigInteger().equals(res)) {
                d.add(new Decoding(encodeFloat(fv, binary), res, FLOAT));
            }

            double dv = it.doubleValue();

            if (Double.isFinite(dv) && BigDecimal.valueOf(dv).toBigInteger().equals(res)) {
                d.add(new Decoding(encodeDouble(dv, binary), res, DOUBLE));
            }

            int bitLength = res.bitLength(), sign = res.signum();

            if (sign > 0) {
                if (bitLength <= Long.SIZE) {
                    d.add(new Decoding(encodeUin64(res.longValue(), binary), res, BIGINT_UNSIGNED));
                }

                if (bitLength <= Integer.SIZE) {
                    d.add(new Decoding(encodeUint(res.intValue(), binary), res, INT_UNSIGNED));
                }

                if (bitLength <= MEDIUM_SIZE) {
                    d.add(new Decoding(encodeInt(res.intValue(), binary), res, MEDIUMINT_UNSIGNED));
                }

                if (bitLength <= Short.SIZE) {
                    d.add(new Decoding(encodeUint16(res.shortValue(), binary), res, SMALLINT_UNSIGNED));
                }

                if (bitLength <= Byte.SIZE) {
                    d.add(new Decoding(encodeUint8(res.byteValue(), binary), res, TINYINT_UNSIGNED));
                }
            }

            if (bitLength < Long.SIZE) {
                d.add(new Decoding(encodeInt64(res.longValueExact(), binary), res, BIGINT));
            }

            if (bitLength < Integer.SIZE) {
                d.add(new Decoding(encodeInt(res.intValueExact(), binary), res, INT));
            }

            if (bitLength < MEDIUM_SIZE) {
                d.add(new Decoding(encodeInt(res.intValueExact(), binary), res, MEDIUMINT));
            }

            if (bitLength < Short.SIZE) {
                d.add(new Decoding(encodeInt16(res.shortValueExact(), binary), res, SMALLINT));
                d.add(new Decoding(encodeInt16(res.shortValueExact(), binary), res, YEAR));
            }

            if (bitLength < Byte.SIZE) {
                d.add(new Decoding(encodeInt8(res.byteValueExact(), binary), res, TINYINT));
            }

            return d.stream();
        }).toArray(Decoding[]::new);
    }

    private ByteBuf convert(BigInteger it) {
        if (it.bitLength() < Long.SIZE) {
            return LongCodecTest.convert(it.longValueExact());
        } else {
            return sized(it);
        }
    }
}
