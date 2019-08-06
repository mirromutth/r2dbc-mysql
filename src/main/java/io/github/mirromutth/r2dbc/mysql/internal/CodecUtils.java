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

package io.github.mirromutth.r2dbc.mysql.internal;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.constant.DataValues.TERMINAL;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.require;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * Common codec methods util.
 */
public final class CodecUtils {

    private CodecUtils() {
    }

    private static final int VAR_INT_1_BYTE_LIMIT = 0xFA;

    private static final int VAR_INT_2_BYTE_LIMIT = (1 << (Byte.SIZE << 1)) - 1;

    private static final short VAR_INT_2_BYTE_CODE = 0xFC;

    private static final int VAR_INT_3_BYTE_LIMIT = (1 << (Byte.SIZE * 3)) - 1;

    private static final short VAR_INT_3_BYTE_CODE = 0xFD;

    /**
     * Visible to test cases.
     */
    static final short VAR_INT_8_BYTE_CODE = 0xFE;

    private static final int MEDIUM_BYTES = 3;

    private static final int MEDIUM_SIZE = MEDIUM_BYTES * Byte.SIZE;

    /**
     * @param buf     C-style string readable buffer.
     * @param charset the string characters' set.
     * @return The string of C-style.
     */
    public static String readCString(ByteBuf buf, Charset charset) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(charset, "charset must not be null");

        int length = buf.bytesBefore(TERMINAL);

        if (length < 0) {
            throw new IllegalArgumentException("buf has no C-style string terminal");
        }

        if (length == 0) {
            // skip terminal
            buf.skipBytes(1);
            return "";
        }

        String result = buf.toString(buf.readerIndex(), length, charset);
        buf.skipBytes(length + 1); // skip string and terminal by read

        return result;
    }

    /**
     * @param buf C-style string readable buffer.
     * @return The string of C-style on byte buffer.
     */
    public static ByteBuf readCStringSlice(ByteBuf buf) {
        requireNonNull(buf, "buf must not be null");

        int length = buf.bytesBefore(TERMINAL);

        if (length < 0) {
            throw new IllegalArgumentException("buf has no C-style string");
        }

        if (length == 0) {
            // skip terminal
            buf.skipBytes(1);
            // use EmptyByteBuf
            return buf.alloc().buffer(0, 0);
        }

        ByteBuf result = buf.readSlice(length);
        buf.skipBytes(1);
        return result;
    }

    public static long crossReadVarInt(ByteBuf firstPart, ByteBuf secondPart) {
        requireNonNull(firstPart, "firstPart must not be null");
        requireNonNull(secondPart, "secondPart must not be null");

        short firstByte = firstPart.readUnsignedByte();

        if (firstByte < VAR_INT_2_BYTE_CODE) {
            return firstByte;
        } else if (firstByte == VAR_INT_2_BYTE_CODE) {
            int readable = firstPart.readableBytes();

            switch (readable) {
                case 0:
                    return secondPart.readUnsignedShortLE();
                case 1:
                    return firstPart.readUnsignedByte() | (secondPart.readUnsignedByte() << Byte.SIZE);
                default:
                    return firstPart.readUnsignedShortLE();
            }
        } else if (firstByte == VAR_INT_3_BYTE_CODE) {
            int readable = firstPart.readableBytes();

            switch (readable) {
                case 0:
                    return secondPart.readUnsignedMediumLE();
                case 1:
                    return firstPart.readUnsignedByte() | (secondPart.readUnsignedShortLE() << Byte.SIZE);
                case 2:
                    return firstPart.readUnsignedShortLE() | (secondPart.readUnsignedByte() << Short.SIZE);
                default:
                    return firstPart.readUnsignedMediumLE();
            }
        } else {
            return crossReadLong0(firstPart, secondPart);
        }
    }

    private static long crossReadLong0(ByteBuf firstPart, ByteBuf secondPart) {
        int readable = firstPart.readableBytes();

        if (readable == 0) {
            return secondPart.readLongLE();
        }

        long low, middle, high;

        switch (readable) {
            case 1:
                low = firstPart.readUnsignedByte();
                middle = (secondPart.readUnsignedIntLE() << Byte.SIZE);
                high = ((long) secondPart.readUnsignedMediumLE()) << (Byte.SIZE + Integer.SIZE);

                return high | middle | low;
            case 2:
                low = firstPart.readUnsignedShortLE();
                middle = secondPart.readUnsignedIntLE() << Short.SIZE;
                high = ((long) secondPart.readUnsignedShortLE()) << (Short.SIZE + Integer.SIZE);

                return high | middle | low;
            case 3:
                low = firstPart.readUnsignedMediumLE();
                middle = secondPart.readUnsignedIntLE() << MEDIUM_SIZE;
                high = ((long) secondPart.readUnsignedByte()) << (MEDIUM_SIZE + Integer.SIZE);

                return high | middle | low;
            case 4:
                low = firstPart.readUnsignedIntLE();
                high = secondPart.readUnsignedIntLE() << Integer.SIZE;

                return high | low;
            case 5:
                low = firstPart.readUnsignedIntLE();
                middle = ((long) firstPart.readUnsignedByte()) << Integer.SIZE;
                high = ((long) secondPart.readUnsignedMediumLE()) << (Integer.SIZE + Byte.SIZE);

                return high | middle | low;
            case 6:
                low = firstPart.readUnsignedIntLE();
                middle = ((long) firstPart.readUnsignedShortLE()) << Integer.SIZE;
                high = ((long) secondPart.readUnsignedShortLE()) << (Integer.SIZE + Short.SIZE);

                return high | middle | low;
            case 7:
                low = firstPart.readUnsignedIntLE();
                middle = ((long) firstPart.readUnsignedMediumLE()) << Integer.SIZE;
                high = ((long) secondPart.readUnsignedByte()) << (Integer.SIZE + MEDIUM_SIZE);

                return high | middle | low;
            default:
                return firstPart.readLongLE();
        }
    }

    /**
     * Note: it will change {@code buf} readerIndex.
     *
     * @param buf a readable buffer include a var integer.
     * @return A var integer read from buffer.
     */
    public static long readVarInt(ByteBuf buf) {
        requireNonNull(buf, "buf must not be null");

        short firstByte = buf.readUnsignedByte();

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
            return byteSize - Byte.BYTES;
        } else if (firstByte == VAR_INT_2_BYTE_CODE) {
            return byteSize - (Byte.BYTES + Short.BYTES);
        } else if (firstByte == VAR_INT_3_BYTE_CODE) {
            return byteSize - (Byte.BYTES + MEDIUM_BYTES);
        } else if (firstByte == VAR_INT_8_BYTE_CODE) {
            return byteSize - (Byte.BYTES + Long.BYTES);
        } else {
            return -1;
        }
    }

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
        requireNonNull(buf, "buf must not be null");

        int size = (int) readVarInt(buf);
        if (size == 0) {
            // use EmptyByteBuf
            return buf.alloc().buffer(0, 0);
        }

        return buf.readSlice(size);
    }

    /**
     * @param buf     that want write to this {@link ByteBuf}
     * @param value   content that want write
     * @param charset {@code value} characters' set
     */
    public static void writeCString(ByteBuf buf, String value, Charset charset) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(value, "value must not be null");
        requireNonNull(charset, "charset must not be null");

        if (!value.isEmpty()) {
            buf.writeCharSequence(value, charset);
        }
        buf.writeByte(TERMINAL);
    }

    public static void writeCString(ByteBuf buf, byte[] value) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(value, "value must not be null");

        if (value.length > 0) {
            buf.writeBytes(value);
        }
        buf.writeByte(TERMINAL);
    }

    /**
     * Set MySQL var integer to {@code buf} from 64-bits integer,
     * the {@code writerIndex} of {@code buf} will be not changes.
     *
     * @param buf   that want set to this {@link ByteBuf}
     * @param value integer that want write
     */
    public static void setVarInt(ByteBuf buf, int writerIndex, long value) {
        requireNonNull(buf, "buf must not be null");
        require(value >= 0, "value must not be a negative integer");

        if (value <= VAR_INT_1_BYTE_LIMIT) { // 0 <= x <= 250, also < 251
            buf.setByte(writerIndex, (int) value);
        } else if (value <= VAR_INT_2_BYTE_LIMIT) { // 251 <= x <= 65535, also < 65536
            buf.setByte(writerIndex, VAR_INT_2_BYTE_CODE)
                .setShortLE(writerIndex + Byte.BYTES, (int) value);
        } else if (value <= VAR_INT_3_BYTE_LIMIT) { // 65536 <= x <= 16777215, also < 16777216
            buf.setByte(writerIndex, VAR_INT_3_BYTE_CODE)
                .setMediumLE(writerIndex + Byte.BYTES, (int) value);
        } else { // x > 16777215
            buf.setByte(writerIndex, VAR_INT_8_BYTE_CODE)
                .setLongLE(writerIndex + Byte.BYTES, value);
        }
    }

    /**
     * Write MySQL var integer to {@code buf} from 64-bits integer.
     *
     * @param buf   that want write to this {@link ByteBuf}
     * @param value integer that want write
     */
    public static void writeVarInt(ByteBuf buf, long value) {
        requireNonNull(buf, "buf must not be null");
        require(value >= 0, "value must not be a negative integer");

        if (value <= VAR_INT_1_BYTE_LIMIT) { // 0 <= x <= 250, also < 251
            buf.writeByte((int) value);
        } else if (value <= VAR_INT_2_BYTE_LIMIT) { // 251 <= x <= 65535, also < 65536
            buf.writeByte(VAR_INT_2_BYTE_CODE).writeShortLE((int) value);
        } else if (value <= VAR_INT_3_BYTE_LIMIT) { // 65536 <= x <= 16777215, also < 16777216
            buf.writeByte(VAR_INT_3_BYTE_CODE).writeMediumLE((int) value);
        } else { // x >= 16777216
            buf.writeByte(VAR_INT_8_BYTE_CODE).writeLongLE(value);
        }
    }

    /**
     * Calculate encoded bytes of a var integer.
     *
     * @param value a integer to calculate encoded bytes.
     * @return encoded bytes length.
     */
    public static int varIntBytes(long value) {
        require(value >= 0, "value must not be a negative integer");

        if (value <= VAR_INT_1_BYTE_LIMIT) { // 0 <= x <= 250, also < 251
            return Byte.BYTES;
        } else if (value <= VAR_INT_2_BYTE_LIMIT) { // 251 <= x <= 65535, also < 65536
            return Byte.BYTES + Short.BYTES;
        } else if (value <= VAR_INT_3_BYTE_LIMIT) { // 65536 <= x <= 16777215, also < 16777216
            return Byte.BYTES + MEDIUM_BYTES;
        } else { // x >= 16777216
            return Byte.BYTES + Long.BYTES;
        }
    }

    public static void writeVarIntSizedString(ByteBuf buf, String value, Charset charset) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(value, "value must not be null");
        requireNonNull(charset, "charset must not be null");

        if (value.isEmpty()) {
            buf.writeByte(0);
        } else {
            // NEVER use value.length() in here, size must be bytes' size, not string size
            writeVarIntSizedBytes(buf, value.getBytes(charset));
        }
    }

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

    public static int readIntInDigits(ByteBuf buf, boolean skipLast) {
        if (!buf.isReadable()) {
            return 0;
        }

        int readerIndex = buf.readerIndex();
        int writerIndex = buf.writerIndex();
        int result = 0;
        byte digit;

        for (int i = readerIndex; i < writerIndex; ++i) {
            digit = buf.getByte(i);

            if (digit >= '0' && digit <= '9') {
                result = result * 10 + (digit - '0');
            } else {
                if (skipLast) {
                    buf.readerIndex(i + 1);
                } else {
                    buf.readerIndex(i);
                }
                // Is not digit, means parse completed.
                return result;
            }
        }

        // Parse until end-of-buffer.
        buf.readerIndex(writerIndex);
        return result;
    }
}
