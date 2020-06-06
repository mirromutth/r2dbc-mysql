/*
 * Copyright 2018-2020 the original author or authors.
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

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Unit tests for {@link IntegerCodec}.
 */
class IntegerCodecTest implements CodecTestSupport<Integer> {

    private final Integer[] integers = {
        0,
        1,
        -1,
        10,
        -10,
        (int) Byte.MIN_VALUE,
        (int) Byte.MAX_VALUE,
        (int) Short.MAX_VALUE,
        (int) Short.MIN_VALUE,
        Integer.MAX_VALUE,
        Integer.MIN_VALUE,
    };

    @Override
    public IntegerCodec getCodec(ByteBufAllocator allocator) {
        return new IntegerCodec(allocator);
    }

    @Override
    public Integer[] originParameters() {
        return integers;
    }

    @Override
    public Object[] stringifyParameters() {
        return integers;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(integers).map(LongCodecTest::encode).toArray(ByteBuf[]::new);
    }

    @Override
    public ByteBuf sized(ByteBuf value) {
        return value;
    }
}
