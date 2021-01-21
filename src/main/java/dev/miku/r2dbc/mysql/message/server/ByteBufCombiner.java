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

import dev.miku.r2dbc.mysql.util.NettyBufferUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.util.List;

/**
 * An utility considers combining {@link ByteBuf}s to a single {@link ByteBuf}.
 */
final class ByteBufCombiner {

    /**
     * Combine {@link ByteBuf}s through composite buffer.
     * <p>
     * This method would release all {@link ByteBuf}s when any exception throws.
     *
     * @param parts The {@link ByteBuf}s want to be wrap, it can not be empty, and it will be cleared.
     * @return A {@link ByteBuf} holds the all bytes of given {@code parts}, it may be a read-only buffer.
     */
    static ByteBuf composite(List<ByteBuf> parts) {
        int size = parts.size();

        switch (size) {
            case 0:
                throw new IllegalStateException("No buffer available");
            case 1:
                try {
                    return parts.get(0);
                } finally {
                    parts.clear();
                }
            default:
                CompositeByteBuf composite = null;

                try {
                    composite = parts.get(0).alloc().compositeBuffer(size);
                    // Auto-releasing failed parts
                    return composite.addComponents(true, parts);
                } catch (Throwable e) {
                    if (composite == null) {
                        // Alloc failed, release parts.
                        NettyBufferUtils.releaseAll(parts);
                    } else {
                        // Also release success parts.
                        composite.release();
                    }
                    throw e;
                } finally {
                    parts.clear();
                }
        }
    }

    private ByteBufCombiner() { }
}
