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
import org.testcontainers.shaded.org.apache.commons.lang.ArrayUtils;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.BitSet;

/**
 * Unit tests for {@link BitSetCodec}.
 */
class BitSetCodecTest implements CodecTestSupport<BitSet> {

    private final BitSet[] sets = {
        BitSet.valueOf(new byte[0]),
        BitSet.valueOf(new byte[] { 0 }), // It is also empty
        BitSet.valueOf(new byte[] { 4, 5, 6 }),
        BitSet.valueOf(new long[] { 0x8D567C913B4F61A2L }),
        BitSet.valueOf(new long[] { 0x8D56700000F61A2L }),
        BitSet.valueOf(new byte[] { (byte) 0xFE, (byte) 0xDC, (byte) 0xBA })
    };

    @Override
    public BitSetCodec getCodec(ByteBufAllocator allocator) {
        return new BitSetCodec(allocator);
    }

    @Override
    public BitSet[] originParameters() {
        return sets;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(sets).map(it -> {
            if (it.isEmpty()) {
                return "b'0'";
            } else {
                byte[] bytes = it.toByteArray();
                ArrayUtils.reverse(bytes);
                String content = Hex.toHexString(bytes);
                return String.format("x'%s'", content.startsWith("0") ? content.substring(1) : content);
            }
        }).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(sets).map(BitSetCodecTest::encode).toArray(ByteBuf[]::new);
    }

    @Override
    public ByteBuf sized(ByteBuf value) {
        return value;
    }

    static ByteBuf encode(BitSet value) {
        if (value.isEmpty()) {
            return Unpooled.wrappedBuffer(new byte[] { 0 });
        }
        return LongCodecTest.encode(value.toLongArray()[0]);
    }
}
