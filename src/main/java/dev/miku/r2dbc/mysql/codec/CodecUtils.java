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
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * A utility considers to general encoding/decoding.
 */
final class CodecUtils {

    private static final String LONG_MAX_VALUE = Long.toString(Long.MAX_VALUE);

    /**
     * Gets the only one type argument of a {@link ParameterizedType}.
     *
     * @param type     the {@link ParameterizedType}.
     * @param rawClass the raw generic class which should have only one type argument.
     * @return the only one type argument as a {@link Class}, or {@code null} if not only one or not a class.
     */
    @Nullable
    static Class<?> getTypeArgument(ParameterizedType type, Class<?> rawClass) {
        Type[] arguments = type.getActualTypeArguments();

        if (arguments.length != 1) {
            return null;
        }

        Type result = arguments[0];

        return type.getRawType() == rawClass && result instanceof Class<?> ? (Class<?>) result : null;
    }

    /**
     * Encodes a {@link String} as a {@link ByteBuf} in ASCII.
     *
     * @param alloc the allocator for create {@link ByteBuf}.
     * @param ascii the {@link String} which should only contain ASCII characters.
     * @return encoded {@link ByteBuf}.
     */
    static ByteBuf encodeAscii(ByteBufAllocator alloc, String ascii) {
        // Using ASCII, so byte size is string length.
        int size = ascii.length();

        if (size == 0) {
            // It is zero of var int, not terminal.
            return alloc.buffer(Byte.BYTES).writeByte(0);
        }

        ByteBuf buf = alloc.buffer(VarIntUtils.varIntBytes(size) + size);

        try {
            VarIntUtils.writeVarInt(buf, size);
            buf.writeCharSequence(ascii, StandardCharsets.US_ASCII);
            return buf;
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    /**
     * Converts an unsigned 64-bits integer to a {@link BigInteger}.
     *
     * @param num an unsigned 64-bits integer to be converted to a {@link BigInteger}.
     * @return the unsigned representation {@link BigInteger}.
     */
    static BigInteger unsignedBigInteger(long num) {
        byte[] bits = new byte[Long.BYTES + 1];

        bits[0] = 0;
        bits[1] = (byte) (num >>> 56);
        bits[2] = (byte) (num >>> 48);
        bits[3] = (byte) (num >>> 40);
        bits[4] = (byte) (num >>> 32);
        bits[5] = (byte) (num >>> 24);
        bits[6] = (byte) (num >>> 16);
        bits[7] = (byte) (num >>> 8);
        bits[8] = (byte) num;

        return new BigInteger(bits);
    }

    /**
     * Parses a positive integer from a {@link String} which should only contain numeric characters.  It
     * will not check if overflow or not.
     *
     * @param num the {@link String}.
     * @return the parsed integer.
     */
    static long parsePositive(String num) {
        long value = 0;
        int size = num.length();

        for (int i = 0; i < size; ++i) {
            value = value * 10L + (num.charAt(i) - '0');
        }

        return value;
    }

    /**
     * Checks if an integer is greater than {@link Long#MAX_VALUE}, which integer is represented by a string.
     *
     * @param num the integer represented by {@link String}.
     * @return if it is greater than {@link Long#MAX_VALUE}.
     */
    static boolean isGreaterThanLongMax(String num) {
        int length = num.length(), maxLength = LONG_MAX_VALUE.length();

        if (length != maxLength) {
            // If length less than max value length, even it is 999...99, it is also less than max value.
            return length > maxLength;
        }

        return num.compareTo(LONG_MAX_VALUE) > 0;
    }

    /**
     * Parses a 32-bits integer from a {@link ByteBuf} in decimal.  It can be a signed integer.
     *
     * @param buf the {@link ByteBuf} which should only contain an integer.
     * @return the parsed integer.
     */
    static int parseInt(ByteBuf buf) {
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

    /**
     * Parses a 64-bits integer from a {@link ByteBuf} in decimal.  It can be a signed integer.
     *
     * @param buf the {@link ByteBuf} which should only contain an integer.
     * @return the parsed integer.
     */
    static long parseLong(ByteBuf buf) {
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

    private CodecUtils() { }
}
