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
 * Unit tests for {@link ByteCodec}.
 */
class ByteCodecTest extends NumericCodecTestSupport<Byte> {

    private final Byte[] bytes = {
        0,
        1,
        -1,
        10,
        -10,
        64,
        -64,
        Byte.MAX_VALUE,
        Byte.MIN_VALUE
    };

    @Override
    public ByteCodec getCodec(ByteBufAllocator allocator) {
        return new ByteCodec(allocator);
    }

    @Override
    public Byte[] originParameters() {
        return bytes;
    }

    @Override
    public Object[] stringifyParameters() {
        return bytes;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(bytes)
            .map(it -> Unpooled.wrappedBuffer(new byte[] { it }))
            .toArray(ByteBuf[]::new);
    }

    @Override
    public Decoding[] decoding(boolean binary, Charset charset) {
        return decimals().flatMap(it -> {
            List<Decoding> d = new ArrayList<>();
            BigInteger integer = it.toBigInteger();
            byte res = integer.byteValue();

            d.add(new Decoding(encodeAscii(it.toString()), res, DECIMAL));

            float fv = it.floatValue();

            if (Float.isFinite(fv) && (byte) fv == res) {
                d.add(new Decoding(encodeFloat(fv, binary), res, FLOAT));
            }

            double dv = it.doubleValue();

            if (Double.isFinite(dv) && (byte) dv == res) {
                d.add(new Decoding(encodeDouble(dv, binary), res, DOUBLE));
            }

            int bitLength = integer.bitLength(), sign = integer.signum();

            if (sign > 0) {
                if (bitLength <= Long.SIZE) {
                    d.add(new Decoding(encodeUin64(integer.longValue(), binary), res, BIGINT_UNSIGNED));
                }

                if (bitLength <= Integer.SIZE) {
                    d.add(new Decoding(encodeUint(integer.intValue(), binary), res, INT_UNSIGNED));
                }

                if (bitLength <= MEDIUM_SIZE) {
                    d.add(new Decoding(encodeInt(integer.intValue(), binary), res, MEDIUMINT_UNSIGNED));
                }

                if (bitLength <= Short.SIZE) {
                    d.add(new Decoding(encodeUint16(integer.shortValue(), binary), res, SMALLINT_UNSIGNED));
                }

                if (bitLength <= Byte.SIZE) {
                    d.add(new Decoding(encodeUint8(integer.byteValue(), binary), res, TINYINT_UNSIGNED));
                }
            }

            if (bitLength < Long.SIZE) {
                d.add(new Decoding(encodeInt64(integer.longValueExact(), binary), res, BIGINT));
            }

            if (bitLength < Integer.SIZE) {
                d.add(new Decoding(encodeInt(integer.intValueExact(), binary), res, INT));
            }

            if (bitLength < MEDIUM_SIZE) {
                d.add(new Decoding(encodeInt(integer.intValueExact(), binary), res, MEDIUMINT));
            }

            if (bitLength < Short.SIZE) {
                d.add(new Decoding(encodeInt16(integer.shortValueExact(), binary), res, SMALLINT));
                d.add(new Decoding(encodeInt16(integer.shortValueExact(), binary), res, YEAR));
            }

            if (bitLength < Byte.SIZE) {
                d.add(new Decoding(encodeInt8(integer.byteValueExact(), binary), res, TINYINT));
            }

            return d.stream();
        }).toArray(Decoding[]::new);
    }
}
