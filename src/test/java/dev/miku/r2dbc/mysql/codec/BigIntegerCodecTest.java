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

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Unit tests for {@link BigIntegerCodec}.
 */
class BigIntegerCodecTest implements CodecTestSupport<BigInteger> {

    private final BigInteger[] integers = {
        BigInteger.ZERO,
        BigInteger.ONE,
        BigInteger.TEN,
        BigInteger.valueOf(Long.MAX_VALUE),
        BigInteger.valueOf(Long.MAX_VALUE).pow(7),
    };

    @Override
    public BigIntegerCodec getCodec(ByteBufAllocator allocator) {
        return new BigIntegerCodec(allocator);
    }

    @Override
    public BigInteger[] originParameters() {
        return integers;
    }

    @Override
    public Object[] stringifyParameters() {
        return integers;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(integers)
            .map(it -> Unpooled.wrappedBuffer(it.toString().getBytes(charset)))
            .toArray(ByteBuf[]::new);
    }
}
