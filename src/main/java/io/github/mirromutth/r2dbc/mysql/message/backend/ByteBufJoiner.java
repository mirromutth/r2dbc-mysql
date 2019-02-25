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

import java.util.List;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A {@link ByteBuf} joiner for {@link BackendMessageDecoder}.
 */
interface ByteBufJoiner {

    ByteBuf join(List<ByteBuf> parts, ByteBuf lastPart);

    static ByteBufJoiner composite(ByteBufAllocator bufAllocator) {
        requireNonNull(bufAllocator, "bufAllocator must not be null");

        return (parts, lastPart) -> {
            if (parts.isEmpty()) {
                return lastPart;
            }

            return bufAllocator.compositeBuffer(parts.size() + 1)
                .addComponents(true, parts)
                .addComponent(true, lastPart);
        };
    }
}
