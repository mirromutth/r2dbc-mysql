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

package dev.miku.r2dbc.mysql.message.server;

import dev.miku.r2dbc.mysql.constant.Envelopes;
import dev.miku.r2dbc.mysql.message.FieldValue;
import dev.miku.r2dbc.mysql.message.LargeFieldValue;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.util.NettyBufferUtils;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.List;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * An implementation of {@link FieldReader} for large result which bytes more than {@link Integer#MAX_VALUE},
 * it would be exists when MySQL server return LOB types (i.e. BLOB, CLOB), LONGTEXT length can be unsigned
 * int32.
 */
final class LargeFieldReader extends AbstractReferenceCounted implements FieldReader {

    private final ByteBuf[] buffers;

    private int currentBufIndex = 0;

    LargeFieldReader(ByteBuf[] buffers) {
        this.buffers = buffers;
    }

    @Override
    public short getUnsignedByte() {
        ByteBuf buf = nonEmptyBuffer();
        return buf.getUnsignedByte(buf.readerIndex());
    }

    @Override
    public void skipOneByte() {
        nonEmptyBuffer().skipBytes(1);
    }

    @Override
    public byte[] readSizeFixedBytes(int length) {
        require(length > 0, "length must be a positive integer");

        ByteBuf buf = nonEmptyBuffer();

        if (buf.readableBytes() >= length) {
            return ByteBufUtil.getBytes(buf.readSlice(length));
        }

        return readBytes(buf, length);
    }

    @Override
    public FieldValue readSizeFixedField(int length) {
        require(length > 0, "length must be a positive integer");

        ByteBuf buf = nonEmptyBuffer();

        if (buf.readableBytes() >= length) {
            return new NormalFieldValue(buf.readRetainedSlice(length));
        }

        return new NormalFieldValue(retainedMerge(buf.alloc(), readSlice(buf, length)));
    }

    @Override
    public FieldValue readVarIntSizedField() {
        ByteBuf currentBuf = nonEmptyBuffer();
        long fieldSize;

        if (VarIntUtils.checkNextVarInt(currentBuf) < 0) {
            ByteBuf nextBuf = this.buffers[currentBufIndex + 1];
            fieldSize = VarIntUtils.crossReadVarInt(currentBuf, nextBuf);
            ++currentBufIndex;
        } else {
            fieldSize = VarIntUtils.readVarInt(currentBuf);
        }

        // Refresh non empty buffer because current buffer has been read.
        currentBuf = nonEmptyBuffer();

        List<ByteBuf> results = readSlice(currentBuf, fieldSize);

        if (fieldSize > Integer.MAX_VALUE) {
            return retainedLargeField(results);
        }

        return new NormalFieldValue(retainedMerge(currentBuf.alloc(), results));
    }

    @Override
    public LargeFieldReader touch(Object hint) {
        for (ByteBuf buffer : buffers) {
            buffer.touch(hint);
        }

        return this;
    }

    @Override
    protected void deallocate() {
        NettyBufferUtils.releaseAll(buffers);
    }

    /**
     * Read a fixed length buffer from {@link #buffers}. The length can be very large, so use {@link ByteBuf}
     * list instead of a single buffer.
     *
     * @param current the current {@link ByteBuf} in {@link #buffers}.
     * @param length  the length of read.
     * @return result buffer list, should NEVER retain any buffer.
     */
    private List<ByteBuf> readSlice(ByteBuf current, long length) {
        ByteBuf buf = current;
        List<ByteBuf> results = new ArrayList<>(Math.max(
            (int) Math.min((length / Envelopes.MAX_ENVELOPE_SIZE) + 2, Byte.MAX_VALUE), 10));
        long totalSize = 0;
        int bufReadable;

        // totalSize + bufReadable <= length
        while (totalSize <= length - (bufReadable = buf.readableBytes())) {
            totalSize += bufReadable;
            // No need readSlice because currentBufIndex will be increment after List pushed.
            results.add(buf);
            buf = this.buffers[++this.currentBufIndex];
        }

        if (length > totalSize) {
            // need bytes = length - `results` real length = length - (totalSize - `buf` length)
            results.add(buf.readSlice((int) (length - totalSize)));
        } // else results has filled by prev buffer, and currentBufIndex is unread for now.

        return results;
    }

    private byte[] readBytes(ByteBuf current, int length) {
        ByteBuf buf = current;
        byte[] result = new byte[length];
        int resultSize = 0;
        int bufReadable;

        // resultIndex + bufReadable <= length
        while (resultSize <= (length - (bufReadable = buf.readableBytes()))) {
            buf.readBytes(result, resultSize, bufReadable);
            resultSize += bufReadable;
            buf = this.buffers[++this.currentBufIndex];
        }

        if (length > resultSize) {
            // need bytes = length - `results` real length = length - (totalSize - `buf` length)
            buf.readBytes(result, resultSize, length - resultSize);
        } // else result has filled by prev buffer, and currentBufIndex is unread for now.

        return result;
    }

    private ByteBuf nonEmptyBuffer() {
        ByteBuf buf = buffers[currentBufIndex];

        while (!buf.isReadable()) {
            // Ignore IndexOutOfBounds because it also happen when buffer read fail.
            buf = buffers[++currentBufIndex];
        }

        return buf;
    }

    private static FieldValue retainedLargeField(List<ByteBuf> parts) {
        int size = parts.size();
        int successSentinel = 0;

        try {
            for (int i = 0; i < size; ++i) {
                parts.get(i).retain();
                successSentinel = i + 1;
            }

            return new LargeFieldValue(parts);
        } catch (Throwable e) {
            if (successSentinel < size) {
                // Retains failed, even not call `FieldValue.of`.
                // So release all retained buffers.
                // Of course, this still does not solve call-stack
                // overflow when calling `FieldValue.of`.
                NettyBufferUtils.releaseAll(parts, successSentinel);
            }

            throw e;
        }
    }

    private static ByteBuf retainedMerge(ByteBufAllocator allocator, List<ByteBuf> parts) {
        int successSentinel = 0;
        int size = parts.size();
        CompositeByteBuf byteBuf = allocator.compositeBuffer(size);

        try {
            for (int i = 0; i < size; ++i) {
                parts.get(i).retain();
                successSentinel = i + 1;
            }

            // Auto-releasing failed Buffer if addComponents called.
            return byteBuf.addComponents(true, parts);
        } catch (Throwable e) {
            // Also release components which append succeed.
            ReferenceCountUtil.safeRelease(byteBuf);

            if (successSentinel < size) {
                // Retains failed, even not call addComponents.
                // So release all retained buffers.
                // Of course, this still does not solve call-stack
                // overflow when calling addComponents.
                NettyBufferUtils.releaseAll(parts, successSentinel);
            }

            throw e;
        }
    }
}
