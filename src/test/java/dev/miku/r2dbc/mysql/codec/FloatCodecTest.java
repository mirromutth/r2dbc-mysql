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
 * Unit tests for {@link FloatCodec}.
 */
class FloatCodecTest implements CodecTestSupport<Float> {

    private final Float[] floats = {
        0.0f,
        1.0f,
        -1.0f,
        1.101f,
        -1.101f,
        Float.MAX_VALUE,
        Float.MIN_VALUE,
        Float.MIN_NORMAL,
        -Float.MIN_NORMAL,
        // Following should not be permitted by MySQL server (i.e. the SQL standard), but also test.
        Float.NaN,
        Float.POSITIVE_INFINITY,
        Float.NEGATIVE_INFINITY,
    };

    @Override
    public FloatCodec getCodec(ByteBufAllocator allocator) {
        return new FloatCodec(allocator);
    }

    @Override
    public Float[] originParameters() {
        return floats;
    }

    @Override
    public Object[] stringifyParameters() {
        return floats;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(floats)
            .map(it -> Unpooled.buffer(Float.BYTES, Float.BYTES).writeFloatLE(it))
            .toArray(ByteBuf[]::new);
    }

    @Override
    public ByteBuf sized(ByteBuf value) {
        return value;
    }
}
