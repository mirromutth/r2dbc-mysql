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

import dev.miku.r2dbc.mysql.util.ServerVersion;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static dev.miku.r2dbc.mysql.constant.EmptyArrays.EMPTY_BYTES;

/**
 * A {@link ByteBuf} wrapper for used by {@link ScalarBlob} or {@link ScalarClob}.
 * <p>
 * Note: the {@link #buf} can only be used once and will be released after use.
 */
final class Node {

    private final AtomicReference<ByteBuf> buf;

    Node(ByteBuf buf) {
        this.buf = new AtomicReference<>(buf);
    }

    CharSequence toCharSequence(int collationId, ServerVersion version) {
        ByteBuf buf = this.buf.getAndSet(null);

        if (buf == null) {
            throw new IllegalStateException("Source has been released");
        }

        try {
            if (!buf.isReadable()) {
                return "";
            }

            return buf.readCharSequence(buf.readableBytes(), CharCollation.fromId(collationId, version).getCharset());
        } finally {
            buf.release();
        }
    }

    ByteBuffer toByteBuffer() {
        ByteBuf buf = this.buf.getAndSet(null);

        if (buf == null) {
            throw new IllegalStateException("Source has been released");
        }

        try {
            if (!buf.isReadable()) {
                return ByteBuffer.wrap(EMPTY_BYTES);
            }

            ByteBuffer result = ByteBuffer.allocate(buf.readableBytes());

            buf.readBytes(result);
            result.flip();

            return result;
        } finally {
            buf.release();
        }
    }

    void safeDispose() {
        ByteBuf buf = this.buf.getAndSet(null);

        if (buf != null) {
            ReferenceCountUtil.safeRelease(buf);
        }
    }

    void dispose() {
        ByteBuf buf = this.buf.getAndSet(null);

        if (buf != null) {
            buf.release();
        }
    }
}
