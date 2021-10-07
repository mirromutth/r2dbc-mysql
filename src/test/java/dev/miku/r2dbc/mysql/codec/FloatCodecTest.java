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
import io.netty.buffer.Unpooled;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

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
 * Unit tests for {@link FloatCodec}.
 */
class FloatCodecTest extends NumericCodecTestSupport<Float> {

    private final Float[] floats = {
        0.0f,
        1.0f,
        -1.0f,
        1.101f,
        -1.101f,
        Float.MAX_VALUE,
        Float.MIN_VALUE,
        Float.MIN_NORMAL,
        -Float.MIN_NORMAL,
        // Following should not be permitted by MySQL server (i.e. the SQL standard), but also test.
        Float.NaN,
        Float.POSITIVE_INFINITY,
        Float.NEGATIVE_INFINITY,
    };

    @Override
    public FloatCodec getCodec(ByteBufAllocator allocator) {
        return new FloatCodec(allocator);
    }

    @Override
    public Float[] originParameters() {
        return floats;
    }

    @Override
    public Object[] stringifyParameters() {
        return floats;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(floats)
            .map(it -> Unpooled.buffer(Float.BYTES, Float.BYTES).writeFloatLE(it))
            .toArray(ByteBuf[]::new);
    }

    @Override
    public Decoding[] decoding(boolean binary, Charset charset) {
        return decimals().flatMap(it -> {
            float res = it.floatValue();

            if (!Float.isFinite(res)) {
                return Stream.empty();
            }

            List<Decoding> d = new ArrayList<>();

            d.add(new Decoding(encodeAscii(it.toString()), res, DECIMAL));
            d.add(new Decoding(encodeFloat(res, binary), res, FLOAT));

            double dv = it.doubleValue();

            if (Double.isFinite(dv) && (float) dv == res) {
                d.add(new Decoding(encodeDouble(dv, binary), res, DOUBLE));
            }

            BigInteger integer = it.toBigInteger();
            float inRes = integer.floatValue();
            int bitLength = integer.bitLength(), sign = integer.signum();

            if (sign > 0) {
                if (bitLength <= Long.SIZE) {
                    d.add(new Decoding(encodeUin64(integer.longValue(), binary), inRes, BIGINT_UNSIGNED));
                }

                if (bitLength <= Integer.SIZE) {
                    d.add(new Decoding(encodeUint(integer.intValue(), binary), inRes, INT_UNSIGNED));
                }

                if (bitLength <= MEDIUM_SIZE) {
                    d.add(new Decoding(encodeInt(integer.intValue(), binary), inRes, MEDIUMINT_UNSIGNED));
                }

                if (bitLength <= Short.SIZE) {
                    d.add(new Decoding(encodeUint16(integer.shortValue(), binary), inRes, SMALLINT_UNSIGNED));
                }

                if (bitLength <= Byte.SIZE) {
                    d.add(new Decoding(encodeUint8(integer.byteValue(), binary), inRes, TINYINT_UNSIGNED));
                }
            }

            if (bitLength < Long.SIZE) {
                d.add(new Decoding(encodeInt64(integer.longValueExact(), binary), inRes, BIGINT));
            }

            if (bitLength < Integer.SIZE) {
                d.add(new Decoding(encodeInt(integer.intValueExact(), binary), inRes, INT));
            }

            if (bitLength < MEDIUM_SIZE) {
                d.add(new Decoding(encodeInt(integer.intValueExact(), binary), inRes, MEDIUMINT));
            }

            if (bitLength < Short.SIZE) {
                d.add(new Decoding(encodeInt16(integer.shortValueExact(), binary), inRes, SMALLINT));
                d.add(new Decoding(encodeInt16(integer.shortValueExact(), binary), inRes, YEAR));
            }

            if (bitLength < Byte.SIZE) {
                d.add(new Decoding(encodeInt8(integer.byteValueExact(), binary), inRes, TINYINT));
            }

            return d.stream();
        }).toArray(Decoding[]::new);
    }
}
