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

package io.github.mirromutth.r2dbc.mysql.message.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.util.List;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A {@link ByteBuf} joiner for {@link BackendMessageDecoder}.
 */
interface ByteBufJoiner {

    /**
     * The implementation is responsible to correctly handle the life-cycle of the given {@link ByteBuf}s, so
     * call {@link ByteBuf#release()} if a {@link ByteBuf} is fully consumed.
     *
     * @param parts All previous {@link ByteBuf}s are relative to the last buffer.
     * @param lastPart The last buffer.
     * @return A {@link ByteBuf} holds the all bytes of given {@code parts} and {@code lastPart}
     */
    ByteBuf join(List<ByteBuf> parts, ByteBuf lastPart);

    static ByteBufJoiner wrapped() {
        return (parts, lastPart) -> {
            int maxIndex = parts.size();

            if (maxIndex <= 0) {
                return lastPart;
            }

            try {
                ByteBuf[] buffers = parts.toArray(new ByteBuf[maxIndex + 1]);
                buffers[maxIndex] = lastPart;
                return Unpooled.wrappedBuffer(buffers);
            } catch (Throwable e) {
                for (ByteBuf part : parts) { // failed, release all buffers in list
                    if (part != null) {
                        part.release();
                    }
                }

                throw e; // throw this exception
            } finally {
                parts.clear(); // no need release when success or has released by exception caught
            }
        };
    }
}
