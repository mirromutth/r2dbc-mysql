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

import io.github.mirromutth.r2dbc.mysql.constant.DataValues;
import io.github.mirromutth.r2dbc.mysql.message.FieldValue;
import io.netty.util.ReferenceCountUtil;

/**
 * An implementation of {@link AbstractRowMessage} for text result.
 */
final class TextRowMessage extends AbstractRowMessage {

    private TextRowMessage(FieldValue[] fields) {
        super(fields);
    }

    @Override
    public boolean isBinary() {
        return false;
    }

    static TextRowMessage decode(FieldReader reader, int totalColumns) {
        FieldValue[] fields = new FieldValue[totalColumns];
        int i = 0;

        try {
            for (; i < totalColumns; ++i) {
                if (DataValues.NULL_VALUE == reader.getUnsignedByte()) {
                    reader.skipOneByte();
                    fields[i] = FieldValue.nullField();
                } else {
                    fields[i] = reader.readVarIntSizedField();
                }
            }

            return new TextRowMessage(fields);
        } catch (Throwable e) {
            for (int j = 0; j < i; ++j) {
                ReferenceCountUtil.safeRelease(fields[j]);
            }
            throw e;
        }
    }

    @Override
    public String toString() {
        // Row data should NOT be printed as this may contain security information.
        // Of course, if user use trace level logs, row data is still be printed by ByteBuf dump.
        return String.format("TextRowMessage{ %d fields }", fieldsCount());
    }
}
