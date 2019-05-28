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
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Row data message.
 */
public final class RowMessage extends AbstractReferenceCounted implements ServerMessage {

    private final ByteBuf[] fields;

    private RowMessage(ByteBuf[] fields) {
        this.fields = requireNonNull(fields, "fields must not be null");
    }

    /**
     * WARNING: each element maybe {@code null} if it's data is nullable.
     *
     * @return the elements of row content
     */
    public ByteBuf[] getFields() {
        return fields;
    }

    static RowMessage decode(ByteBuf buf, TextResultDecodeContext context) {
        int size = context.getTotalColumns();
        ByteBuf[] fields = new ByteBuf[size];

        for (int i = 0; i < size; ++i) {
            if (DataValues.NULL_VALUE == buf.getUnsignedByte(buf.readerIndex())) {
                buf.skipBytes(1);
            } else {
                fields[i] = CodecUtils.readVarIntSizedSlice(buf).retain();
            }
        }

        return new RowMessage(fields);
    }

    @Override
    protected void deallocate() {
        for (ByteBuf field : fields) {
            field.release();
        }
    }

    @Override
    public RowMessage touch(@Nullable Object o) {
        for (ByteBuf field : fields) {
            field.touch(o);
        }

        return this;
    }

    @Override
    public String toString() {
        // Row data should NOT be printed as this may contain security information.
        // Of course, if user use trace level logs, row data is still be printed by ByteBuf dump.
        return "RowMessage{fields=<hidden>}";
    }
}
