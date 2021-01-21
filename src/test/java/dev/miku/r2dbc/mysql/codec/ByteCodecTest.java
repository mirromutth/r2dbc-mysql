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

package dev.miku.r2dbc.mysql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Unit tests for {@link ByteCodec}.
 */
class ByteCodecTest implements CodecTestSupport<Byte> {

    private final Byte[] bytes = {
        0,
        1,
        -1,
        10,
        -10,
        64,
        -64,
        Byte.MAX_VALUE,
        Byte.MIN_VALUE
    };

    @Override
    public ByteCodec getCodec(ByteBufAllocator allocator) {
        return new ByteCodec(allocator);
    }

    @Override
    public Byte[] originParameters() {
        return bytes;
    }

    @Override
    public Object[] stringifyParameters() {
        return bytes;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(bytes)
            .map(it -> Unpooled.wrappedBuffer(new byte[] { it }))
            .toArray(ByteBuf[]::new);
    }

    @Override
    public ByteBuf sized(ByteBuf value) {
        return value;
    }
}
