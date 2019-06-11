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

package io.github.mirromutth.r2dbc.mysql.message.server;

import io.github.mirromutth.r2dbc.mysql.message.FieldValue;

/**
 * An implementation of {@link AbstractRowMessage} for binary result.
 */
final class BinaryRowMessage extends AbstractRowMessage {

    private static final byte BIT_MASK_INIT = 1 << 2;

    private BinaryRowMessage(FieldValue[] fields) {
        super(fields);
    }

    @Override
    public boolean isBinary() {
        return true;
    }

    static BinaryRowMessage decode(FieldReader reader, ResultDecodeContext context) {
        reader.skipOneByte(); // constant 0x00
        int totalColumns = context.getTotalColumns();
        // MySQL will make sure columns less than 4096, no need check overflow.
        byte[] nullBitmap = reader.readSizeFixedBytes((totalColumns + 9) >> 3);
        int bitmapIndex = 0;
        byte bitMask = BIT_MASK_INIT;
        FieldValue[] fields = new FieldValue[totalColumns];

        for (int i = 0; i < totalColumns; ++i) {
            if ((nullBitmap[bitmapIndex] & bitMask) != 0) {
                fields[i] = FieldValue.nullField();
            } else {
                int bytes = context.getType(i).getFixedBinaryBytes();
                if (bytes > 0) {
                    fields[i] = reader.readSizeFixedField(bytes);
                } else {
                    fields[i] = reader.readVarIntSizedField();
                }
            }

            bitMask <<= 1;

            // Should make sure it is 0 of byte, it may change to int in future, so try not use `bitMask == 0` only.
            if ((bitMask & 0xFF) == 0) {
                // An approach to circular left shift 1-bit.
                bitMask = 1;
                // Current byte has been completed by read.
                ++bitmapIndex;
            }
        }

        return new BinaryRowMessage(fields);
    }

    @Override
    public String toString() {
        // Row data should NOT be printed as this may contain security information.
        // Of course, if user use trace level logs, row data is still be printed by ByteBuf dump.
        return String.format("BinaryRowMessage{ %d fields }", fieldsCount());
    }
}
