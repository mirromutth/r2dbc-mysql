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

package dev.miku.r2dbc.mysql.codec.lob;

import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.util.ServerVersion;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;

import java.nio.ByteBuffer;

import static dev.miku.r2dbc.mysql.constant.EmptyArrays.EMPTY_BYTES;

/**
 * A {@link ByteBuf} wrapper for used by {@code Blob} or {@code Clob}.
 * <p>
 * Note: the {@link #buf} can only be used once and will be released after use.
 */
final class Node implements ByteBufHolder {

    private final ByteBuf buf;

    Node(ByteBuf buf) {
        this.buf = buf;
    }

    CharSequence readCharSequence(int collationId, ServerVersion version) {
        try {
            if (!buf.isReadable()) {
                return "";
            }

            return buf.readCharSequence(buf.readableBytes(), CharCollation.fromId(collationId, version).getCharset());
        } finally {
            buf.release();
        }
    }

    ByteBuffer readByteBuffer() {
        try {
            if (!buf.isReadable()) {
                return ByteBuffer.wrap(EMPTY_BYTES);
            }

            // Maybe allocateDirect?
            ByteBuffer result = ByteBuffer.allocate(buf.readableBytes());

            buf.readBytes(result);
            result.flip();

            return result;
        } finally {
            buf.release();
        }
    }

    public ByteBuf content() {
        int refCnt = buf.refCnt();

        if (refCnt > 0) {
            return buf;
        } else {
            throw new IllegalReferenceCountException(refCnt);
        }
    }

    public Node copy() {
        return replace(buf.copy());
    }

    public Node duplicate() {
        return replace(buf.duplicate());
    }

    public Node retainedDuplicate() {
        return replace(buf.retainedDuplicate());
    }

    public Node replace(ByteBuf content) {
        return new Node(content);
    }

    public int refCnt() {
        return buf.refCnt();
    }

    public Node retain() {
        buf.retain();
        return this;
    }

    public Node retain(int increment) {
        buf.retain(increment);
        return this;
    }

    public Node touch() {
        buf.touch();
        return this;
    }

    public Node touch(Object hint) {
        buf.touch(hint);
        return this;
    }

    public boolean release() {
        return buf.release();
    }

    public boolean release(int decrement) {
        return buf.release(decrement);
    }

    static void releaseAll(Node[] nodes) {
        for (Node node : nodes) {
            if (node != null) {
                ReferenceCountUtil.safeRelease(node.buf);
            }
        }
    }
}
