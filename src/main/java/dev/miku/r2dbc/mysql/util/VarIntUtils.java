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

package dev.miku.r2dbc.mysql.util;

import io.netty.buffer.ByteBuf;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An utility for encoding/decoding var integer.
 * <p>
 * TODO: add more unit tests.
 */
public final class VarIntUtils {

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

    /**
     * Calculate encoded bytes of a var integer.
     * <p>
     * Note: it will NOT check {@code value} validation.
     *
     * @param value a integer to calculate encoded bytes.
     * @return encoded bytes length.
     */
    public static int varIntBytes(int value) {
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

    /**
     * Reserve a seat of an unknown var integer in {@code buf} header.
     * <p>
     * Note: make sure the var integer will be set into the {@code buf} header,
     * can not use it when you want write a var integer into a {@code buf}
     * which has data before the var integer. i.e. the {@code buf} should be a
     * new {@link ByteBuf}.
     *
     * @param buf that want reserve to this {@link ByteBuf}.
     */
    public static void reserveVarInt(ByteBuf buf) {
        requireNonNull(buf, "buf must not be null");
        // Max size of var int, fill zero to protect memory data.
        buf.writeZero(Byte.BYTES + Long.BYTES);
    }

    /**
     * Set a var integer to the {@code buf} header from 32-bits integer,
     * the {@code buf} header should be reserved by {@link #reserveVarInt}.
     *
     * @param buf   that want set to this {@link ByteBuf}, which is reserved a var integer.
     * @param value integer that want to set.
     * @return the {@code buf} which is set a var integer to the reserved seat.
     */
    public static ByteBuf setReservedVarInt(ByteBuf buf, int value) {
        requireNonNull(buf, "buf must not be null");
        require(value >= 0, "value must not be a negative integer");

        if (value <= VAR_INT_1_BYTE_LIMIT) { // 0 <= x <= 250, skip 8 bytes, over 1 byte.
            int index = buf.readerIndex() + Long.BYTES;

            return buf.setByte(index, value).skipBytes(Long.BYTES);
        } else if (value <= VAR_INT_2_BYTE_LIMIT) { // 251 <= x <= 65535, skip 6 bytes, over 3 bytes.
            int index = buf.readerIndex() + (Long.BYTES - Short.BYTES);

            return buf.setByte(index, VAR_INT_2_BYTE_CODE)
                .setShortLE(index + Byte.BYTES, value)
                .skipBytes(Long.BYTES - Short.BYTES);
        } else if (value <= VAR_INT_3_BYTE_LIMIT) { // 65536 <= x <= 16777215, skip 5 bytes, over 4 bytes.
            int index = buf.readerIndex() + (Long.BYTES - MEDIUM_BYTES);

            return buf.setByte(index, VAR_INT_3_BYTE_CODE)
                .setMediumLE(index + Byte.BYTES, value)
                .skipBytes(Long.BYTES - MEDIUM_BYTES);
        } else { // x >= 16777216, no need skip, over 9 bytes.
            int index = buf.readerIndex();

            return buf.setByte(index, VAR_INT_8_BYTE_CODE)
                .setLongLE(index + Byte.BYTES, value);
        }
    }

    /**
     * Set a var integer to the {@code buf} header from 64-bits integer,
     * the {@code buf} header should be reserved by {@link #reserveVarInt}.
     *
     * @param buf   that want set to this {@link ByteBuf}, which is reserved a var integer.
     * @param value integer that want to set.
     */
    public static void setReservedVarInt(ByteBuf buf, long value) {
        requireNonNull(buf, "buf must not be null");
        require(value >= 0, "value must not be a negative integer");

        if (value <= VAR_INT_1_BYTE_LIMIT) { // 0 <= x <= 250, skip 8 bytes, over 1 byte.
            int index = buf.readerIndex() + Long.BYTES;

            buf.setByte(index, (int) value).skipBytes(Long.BYTES);
        } else if (value <= VAR_INT_2_BYTE_LIMIT) { // 251 <= x <= 65535, skip 6 bytes, over 3 bytes.
            int index = buf.readerIndex() + (Long.BYTES - Short.BYTES);

            buf.setByte(index, VAR_INT_2_BYTE_CODE)
                .setShortLE(index + Byte.BYTES, (int) value)
                .skipBytes(Long.BYTES - Short.BYTES);
        } else if (value <= VAR_INT_3_BYTE_LIMIT) { // 65536 <= x <= 16777215, skip 5 bytes, over 4 bytes.
            int index = buf.readerIndex() + (Long.BYTES - MEDIUM_BYTES);

            buf.setByte(index, VAR_INT_3_BYTE_CODE)
                .setMediumLE(index + Byte.BYTES, (int) value)
                .skipBytes(Long.BYTES - MEDIUM_BYTES);
        } else { // x >= 16777216, no need skip, over 9 bytes.
            int index = buf.readerIndex();

            buf.setByte(index, VAR_INT_8_BYTE_CODE)
                .setLongLE(index + Byte.BYTES, value);
        }
    }

    /**
     * Write MySQL var integer to {@code buf} from 32-bits integer.
     *
     * @param buf   that want write to this {@link ByteBuf}
     * @param value integer that want write
     */
    public static void writeVarInt(ByteBuf buf, int value) {
        requireNonNull(buf, "buf must not be null");
        require(value >= 0, "value must not be a negative integer");

        if (value <= VAR_INT_1_BYTE_LIMIT) { // 0 <= x <= 250, also < 251
            buf.writeByte(value);
        } else if (value <= VAR_INT_2_BYTE_LIMIT) { // 251 <= x <= 65535, also < 65536
            buf.writeByte(VAR_INT_2_BYTE_CODE).writeShortLE(value);
        } else if (value <= VAR_INT_3_BYTE_LIMIT) { // 65536 <= x <= 16777215, also < 16777216
            buf.writeByte(VAR_INT_3_BYTE_CODE).writeMediumLE(value);
        } else { // x >= 16777216
            buf.writeByte(VAR_INT_8_BYTE_CODE).writeLongLE(value);
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

    private VarIntUtils() {
    }
}
