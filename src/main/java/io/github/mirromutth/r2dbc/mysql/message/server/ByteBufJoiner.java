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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.util.List;

/**
 * A {@link ByteBuf} joiner for {@link ServerMessageDecoder}.
 */
interface ByteBufJoiner {

    /**
     * This method would release all {@link ByteBuf}s when any exception throws.
     *
     * @param parts {@link ByteBuf}s want to be wrap, it will be clear even exception happened.
     * @return A {@link ByteBuf} holds the all bytes of given {@code parts}
     */
    ByteBuf join(List<ByteBuf> parts);

    static ByteBufJoiner wrapped() {
        return (parts) -> {
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
                            for (ByteBuf part : parts) {
                                ReferenceCountUtil.safeRelease(part);
                            }
                        } else {
                            // Also release success parts.
                            composite.release();
                        }
                        throw e;
                    } finally {
                        parts.clear();
                    }
            }
        };
    }
}
