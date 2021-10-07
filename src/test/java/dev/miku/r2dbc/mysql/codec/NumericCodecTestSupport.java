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

import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Base class considers unit tests for numeric types {@link Codec}.
 */
abstract class NumericCodecTestSupport<T extends Number> implements CodecTestSupport<T> {

    protected static final int MEDIUM_SIZE = 24;

    private static final String[] NUMBERS = {
        "0",
        "-1",
        "1",
        "1.5", // Safety as float
        "123.456", // Safety as double
        "1.20",
        "3.45e6",
        "-4.156e3",
        "8941.14560100",
        Byte.toString(Byte.MIN_VALUE),
        Byte.toString(Byte.MAX_VALUE),
        Short.toString((short) -256),
        Short.toString((short) 255),
        Short.toString(Short.MIN_VALUE),
        Short.toString(Short.MAX_VALUE),
        Integer.toString(-65536),
        Integer.toString(65535),
        Integer.toString(-1 << 23), // Int24 minimum value
        Integer.toString(~(-1 << 23)), // Int24 maximum value
        Integer.toString(-1 << 24),
        Integer.toString(~(-1 << 24)),
        Integer.toString(Integer.MIN_VALUE),
        Integer.toString(Integer.MAX_VALUE),
        Long.toString(~Integer.toUnsignedLong(-1)),
        Integer.toUnsignedString(-1),
        Long.toString(Long.MIN_VALUE),
        Long.toString(Long.MAX_VALUE),
        Long.toUnsignedString(Long.MIN_VALUE),
        Long.toUnsignedString(-1),
        '-' + Long.toUnsignedString(-1),
        "-4987456123135798415631687856414894789415623165",
        "4987456123135798415631687856414894789415623165",
        "156748914896454875412364854198619981491165.00145612",
        Float.toString(-Float.MIN_NORMAL),
        Float.toString(Float.MIN_NORMAL),
        Float.toString(Float.MIN_VALUE),
        Float.toString(Float.MAX_VALUE),
        Double.toString(-Double.MIN_NORMAL),
        Double.toString(Double.MIN_NORMAL),
        Double.toString(Double.MIN_VALUE),
        Double.toString(Double.MAX_VALUE),
        // Currently, there is no need to make a test case larger than the maximum double precision value.
        // MySQL DECIMAL(M, D), where M is the maximum precision number. It is between 1 and 65.
    };

    @Override
    public ByteBuf sized(ByteBuf value) {
        return value;
    }

    protected ByteBuf sized(T value) {
        String s = value.toString();
        ByteBuf buf = Unpooled.buffer();
        try {
            VarIntUtils.writeVarInt(buf, s.length());
            buf.writeCharSequence(s, StandardCharsets.US_ASCII);
            return buf;
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    protected ByteBuf encodeFloat(float value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Float.BYTES, Float.BYTES).writeFloatLE(value);
        }

        // Keep more precisions.
        return encodeAscii(Double.toString(value));
    }

    protected ByteBuf encodeDouble(double value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Double.BYTES, Double.BYTES).writeDoubleLE(value);
        }

        return encodeAscii(Double.toString(value));
    }

    protected ByteBuf encodeUin64(long value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Long.BYTES, Long.BYTES).writeLongLE(value);
        }

        return encodeAscii(Long.toUnsignedString(value));
    }

    protected ByteBuf encodeUint(int value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Integer.BYTES, Integer.BYTES).writeIntLE(value);
        }

        return encodeAscii(Integer.toUnsignedString(value));
    }

    protected ByteBuf encodeUint16(short value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Short.BYTES, Short.BYTES).writeShortLE(value);
        }

        return encodeAscii(Integer.toString(Short.toUnsignedInt(value)));
    }

    protected ByteBuf encodeUint8(byte value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Byte.BYTES, Byte.BYTES).writeByte(value);
        }

        return encodeAscii(Integer.toString(Byte.toUnsignedInt(value)));
    }

    protected ByteBuf encodeInt64(long value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Long.BYTES, Long.BYTES).writeLongLE(value);
        }

        return encodeAscii(Long.toString(value));
    }

    protected ByteBuf encodeInt(int value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Integer.BYTES, Integer.BYTES).writeIntLE(value);
        }

        return encodeAscii(Integer.toString(value));
    }

    protected ByteBuf encodeInt16(short value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Short.BYTES, Short.BYTES).writeShortLE(value);
        }

        return encodeAscii(Short.toString(value));
    }

    protected ByteBuf encodeInt8(byte value, boolean binary) {
        if (binary) {
            return Unpooled.buffer(Byte.BYTES, Byte.BYTES).writeByte(value);
        }

        return encodeAscii(Byte.toString(value));
    }

    protected Stream<BigDecimal> decimals() {
        return Arrays.stream(NUMBERS).map(BigDecimal::new);
    }
}
