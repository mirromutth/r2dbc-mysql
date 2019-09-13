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

package dev.miku.r2dbc.mysql.message.server;

import dev.miku.r2dbc.mysql.internal.AssertUtils;
import dev.miku.r2dbc.mysql.message.FieldValue;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

import java.util.List;

/**
 * A field reader considers read {@link FieldValue}s from {@link ByteBuf}(s).
 */
interface FieldReader extends ReferenceCounted {

    /**
     * It will not change reader index.
     *
     * @return current byte of unsigned.
     */
    short getUnsignedByte();

    void skipOneByte();

    /**
     * @param length must be a positive integer.
     * @return length fixed field.
     */
    byte[] readSizeFixedBytes(int length);

    /**
     * @param length must be a positive integer.
     * @return length fixed field.
     */
    FieldValue readSizeFixedField(int length);

    FieldValue readVarIntSizedField();

    @SuppressWarnings("ForLoopReplaceableByForEach")
    static FieldReader of(ByteBufJoiner joiner, List<ByteBuf> buffers) {
        AssertUtils.requireNonNull(joiner, "joiner must not be null");
        AssertUtils.requireNonNull(buffers, "buffers must not be null");

        int size = buffers.size();
        long totalSize = 0;

        try {
            for (int i = 0; i < size; ++i) {
                totalSize += buffers.get(i).readableBytes();

                if (totalSize > Integer.MAX_VALUE) {
                    break;
                }
            }
        } catch (Throwable e) {
            for (int i = 0; i < size; ++i) {
                ReferenceCountUtil.safeRelease(buffers.get(i));
            }
            buffers.clear();
            throw e;
        }

        if (totalSize <= Integer.MAX_VALUE) {
            // Netty ByteBuf max length is Integer.MAX_VALUE.
            ByteBuf joined = joiner.join(buffers);
            try {
                // Reader will release `joined` by close if create succeed.
                return new NormalFieldReader(joined);
            } catch (Throwable e) {
                joined.release();
                throw e;
            } finally {
                buffers.clear();
            }
        } else {
            try {
                return new LargeFieldReader(buffers.toArray(new ByteBuf[size]));
            } catch (Throwable e) {
                for (int i = 0; i < size; ++i) {
                    ReferenceCountUtil.safeRelease(buffers.get(i));
                }
                throw e;
            } finally {
                buffers.clear();
            }
        }
    }
}
