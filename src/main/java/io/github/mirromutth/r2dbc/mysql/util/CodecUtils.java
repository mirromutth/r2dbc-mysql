/*
 * Copyright 2018-2019 the original author or authors.
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

package io.github.mirromutth.r2dbc.mysql.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;

import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNegative;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Common codec methods util.
 */
public final class CodecUtils {

    private CodecUtils() {
    }

    private static final int VAR_INT_1_BYTE_LIMIT = 0xFA;

    private static final int VAR_INT_2_BYTE_LIMIT = (1 << (Byte.SIZE << 1)) - 1;

    private static final int VAR_INT_2_BYTE_CODE = 0xFC;

    private static final int VAR_INT_3_BYTE_LIMIT = (1 << (Byte.SIZE * 3)) - 1;

    private static final int VAR_INT_3_BYTE_CODE = 0xFD;

    private static final int VAR_INT_8_BYTE_CODE = 0xFE;

    private static final int MEDIUM_BYTES = 3;

    private static final byte TERMINAL = 0;

    /**
     * @param buf     C-style string readable buffer.
     * @param charset the string characters' set.
     * @return The string of C-style.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static String readCString(ByteBuf buf, Charset charset) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(charset, "charset must not be null");

        int length = buf.bytesBefore(TERMINAL);

        String result = buf.toString(buf.readerIndex(), length, charset);
        buf.skipBytes(length + 1); // skip string and terminal by read

        return result;
    }

    /**
     * @param buf C-style string readable buffer.
     * @return The string of C-style on byte buffer.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static ByteBuf readCStringSlice(ByteBuf buf) {
        requireNonNull(buf, "buf must not be null");

        ByteBuf result = buf.readSlice(buf.bytesBefore(TERMINAL));
        buf.skipBytes(1);
        return result;
    }

    /**
     * @param buf a readable buffer include a var integer.
     * @return A var integer read from buffer.
     */
    public static long readVarInt(ByteBuf buf) {
        short firstByte = requireNonNull(buf, "buf must not be null").readUnsignedByte();

        if (firstByte < VAR_INT_2_BYTE_CODE) {
            return firstByte;
        } else if (firstByte == VAR_INT_2_BYTE_CODE) {
            return buf.readUnsignedShortLE();
        } else if (firstByte == VAR_INT_3_BYTE_CODE) {
            return buf.readUnsignedMediumLE();
        } else {
            return buf.readLongLE();
        }
    }

    public static boolean hasNextCString(ByteBuf buf) {
        return buf.bytesBefore(TERMINAL) >= 0;
    }

    /**
     * @param buf a readable buffer for check it has next a var integer or not.
     * @return a negative integer means {@code buf} has not a var integer,
     * return 0 means {@code buf} looks like has only a var integer which buffer has no other data,
     * return a positive integer means how much buffer size after read a var integer.
     */
    public static int checkNextVarInt(ByteBuf buf) {
        int byteSize = requireNonNull(buf, "buf must not be null").readableBytes();

        if (byteSize <= 0) {
            return -1;
        }

        short firstByte = buf.getUnsignedByte(buf.readerIndex());

        if (firstByte < VAR_INT_2_BYTE_CODE) {
            return byteSize - 1;
        } else if (firstByte == VAR_INT_2_BYTE_CODE) {
            return byteSize - (1 + Short.BYTES);
        } else if (firstByte == VAR_INT_3_BYTE_CODE) {
            return byteSize - (1 + MEDIUM_BYTES);
        } else if (firstByte == VAR_INT_8_BYTE_CODE) {
            return byteSize - (1 + Long.BYTES);
        } else {
            return -1;
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static String readVarIntSizedString(ByteBuf buf, Charset charset) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(charset, "charset must not be null");

        int size = (int) readVarInt(buf); // JVM can NOT support string which length upper than maximum of int32

        if (size == 0) {
            return "";
        }

        String result = buf.toString(buf.readerIndex(), size, charset);
        buf.skipBytes(size);

        return result;
    }

    public static ByteBuf readVarIntSizedSlice(ByteBuf buf) {
        // JVM can NOT support string which length upper than maximum of int32
        int size = (int) readVarInt(requireNonNull(buf, "buf must not be null"));

        if (size == 0) {
            return new EmptyByteBuf(buf.alloc());
        }

        return buf.readSlice(size);
    }

    /**
     * @param buf     that want write to this {@link ByteBuf}
     * @param value   content that want write
     * @param charset {@code value} characters' set
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void writeCString(ByteBuf buf, CharSequence value, Charset charset) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(value, "value must not be null");
        requireNonNull(charset, "charset must not be null");

        buf.writeCharSequence(value, charset);
        buf.writeByte(TERMINAL);
    }

    /**
     * Write MySQL var int to {@code buf} for 32-bits integer.
     * <p>
     * WARNING: it is MySQL var int (size encoded integer),
     * that is not like var int usually.
     *
     * @param buf   that want write to this {@link ByteBuf}
     * @param value integer that want write
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void writeVarInt(ByteBuf buf, int value) {
        requireNonNull(buf, "buf must not be null");
        requireNonNegative(value, "value must not be negative");

        // if it greater than 3 bytes limit, it must be 8 bytes integer, this is MySQL var int rule
        if (value > VAR_INT_3_BYTE_LIMIT) {
            buf.writeByte(VAR_INT_8_BYTE_CODE).writeLongLE(value);
        } else if (value > VAR_INT_2_BYTE_LIMIT) {
            buf.writeByte(VAR_INT_3_BYTE_CODE).writeMediumLE(value);
        } else if (value > VAR_INT_1_BYTE_LIMIT) {
            buf.writeByte(VAR_INT_2_BYTE_CODE).writeShortLE(value);
        } else {
            buf.writeByte(value);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void writeVarIntSizedString(ByteBuf buf, String value, Charset charset) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(value, "value must not be null");
        requireNonNull(charset, "charset must not be null");

        // NEVER use value.length() in here, size must be bytes' size, not string size
        writeVarIntSizedBytes(buf, value.getBytes(charset));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void writeVarIntSizedBytes(ByteBuf buf, byte[] value) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(value, "value must not be null");

        int size = value.length;
        if (size > 0) {
            writeVarInt(buf, size);
            buf.writeBytes(value);
        } else {
            buf.writeByte(0);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void writeVarIntSizedBytes(ByteBuf buf, ByteBuf value) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(value, "value must not be null");

        int size = value.readableBytes();
        if (size > 0) {
            writeVarInt(buf, size);
            buf.writeBytes(value);
        } else {
            buf.writeByte(0);
        }
    }
}
