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

package dev.miku.r2dbc.mysql.codec.lob;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Blob;

import java.nio.ByteBuffer;

import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_BYTES;

/**
 * An implementation of {@link Blob} for singleton {@link ByteBuf}.
 */
final class SingletonBlob extends SingletonLob<ByteBuffer> implements Blob {

    SingletonBlob(ByteBuf buf) {
        super(buf);
    }

    @Override
    protected ByteBuffer convert(ByteBuf buf) {
        if (!buf.isReadable()) {
            return ByteBuffer.wrap(EMPTY_BYTES);
        }

        // Maybe allocateDirect?
        ByteBuffer result = ByteBuffer.allocate(buf.readableBytes());

        buf.readBytes(result);
        result.flip();

        return result;
    }
}
