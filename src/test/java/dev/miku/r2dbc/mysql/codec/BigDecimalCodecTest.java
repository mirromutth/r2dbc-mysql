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

/**
 * Unit tests for {@link BigDecimalCodec}.
 */
class BigDecimalCodecTest extends NumericCodecTestSupport<BigDecimal> {

    private final BigDecimal[] decimals = {
        BigDecimal.ZERO,
        BigDecimal.ONE,
        BigDecimal.TEN,
        new BigDecimal("-1.00"),
        new BigDecimal("-10.010"),
        BigDecimal.valueOf(-2021),
        BigDecimal.valueOf(Long.MAX_VALUE),
        BigDecimal.valueOf(Long.MIN_VALUE),
        BigDecimal.valueOf(Double.MAX_VALUE),
        BigDecimal.valueOf(Double.MAX_VALUE).pow(5),
    };

    @Override
    public BigDecimalCodec getCodec(ByteBufAllocator allocator) {
        return new BigDecimalCodec(allocator);
    }

    @Override
    public BigDecimal[] originParameters() {
        return decimals;
    }

    @Override
    public Object[] stringifyParameters() {
        return decimals;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(decimals).map(this::sized).toArray(ByteBuf[]::new);
    }

    @Override
    public Decoding[] decoding(boolean binary, Charset charset) {
        return decimals().flatMap(it -> {
            List<Decoding> d = new ArrayList<>();

            d.add(new Decoding(encodeAscii(it.toString()), it, DECIMAL));

            float fv = it.floatValue();

            if (Float.isFinite(fv) && BigDecimal.valueOf(fv).equals(it)) {
                d.add(new Decoding(encodeFloat(fv, binary), it, FLOAT));
            }

            double dv = it.doubleValue();

            if (Double.isFinite(dv) && BigDecimal.valueOf(dv).equals(it)) {
                d.add(new Decoding(encodeDouble(dv, binary), it, DOUBLE));
            }

            if (isFractional(it)) {
                return d.stream();
            }

            BigInteger integer = it.toBigInteger();
            int bitLength = integer.bitLength(), sign = integer.signum();

            if (sign > 0) {
                if (bitLength <= Long.SIZE) {
                    d.add(new Decoding(encodeUin64(integer.longValue(), binary), it, BIGINT_UNSIGNED));
                }

                if (bitLength <= Integer.SIZE) {
                    d.add(new Decoding(encodeUint(integer.intValue(), binary), it, INT_UNSIGNED));
                }

                if (bitLength <= MEDIUM_SIZE) {
                    d.add(new Decoding(encodeInt(integer.intValue(), binary), it, MEDIUMINT_UNSIGNED));
                }

                if (bitLength <= Short.SIZE) {
                    d.add(new Decoding(encodeUint16(integer.shortValue(), binary), it, SMALLINT_UNSIGNED));
                }

                if (bitLength <= Byte.SIZE) {
                    d.add(new Decoding(encodeUint8(integer.byteValue(), binary), it, TINYINT_UNSIGNED));
                }
            }

            if (bitLength < Long.SIZE) {
                d.add(new Decoding(encodeInt64(integer.longValueExact(), binary), it, BIGINT));
            }

            if (bitLength < Integer.SIZE) {
                d.add(new Decoding(encodeInt(integer.intValueExact(), binary), it, INT));
            }

            if (bitLength < MEDIUM_SIZE) {
                d.add(new Decoding(encodeInt(integer.intValueExact(), binary), it, MEDIUMINT));
            }

            if (bitLength < Short.SIZE) {
                d.add(new Decoding(encodeInt16(integer.shortValueExact(), binary), it, SMALLINT));
                d.add(new Decoding(encodeInt16(integer.shortValueExact(), binary), it, YEAR));
            }

            if (bitLength < Byte.SIZE) {
                d.add(new Decoding(encodeInt8(integer.byteValueExact(), binary), it, TINYINT));
            }

            return d.stream();
        }).toArray(Decoding[]::new);
    }

    private static boolean isFractional(BigDecimal decimal) {
        return !decimal.remainder(BigDecimal.ONE).equals(BigDecimal.ZERO);
    }
}
