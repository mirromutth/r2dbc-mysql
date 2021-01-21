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
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Unit tests for {@link LongCodec}.
 */
class LongCodecTest implements CodecTestSupport<Long> {

    private final Long[] longs = {
        0L,
        1L,
        -1L,
        10L,
        -10L,
        (long) Byte.MIN_VALUE,
        (long) Byte.MAX_VALUE,
        (long) Short.MIN_VALUE,
        (long) Short.MAX_VALUE,
        (long) Integer.MIN_VALUE,
        (long) Integer.MAX_VALUE,
        Long.MIN_VALUE,
        Long.MAX_VALUE
    };

    @Override
    public LongCodec getCodec(ByteBufAllocator allocator) {
        return new LongCodec(allocator);
    }

    @Override
    public Long[] originParameters() {
        return longs;
    }

    @Override
    public Object[] stringifyParameters() {
        return longs;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(longs).map(LongCodecTest::encode).toArray(ByteBuf[]::new);
    }

    @Override
    public ByteBuf sized(ByteBuf value) {
        return value;
    }

    static ByteBuf encode(long value) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return Unpooled.wrappedBuffer(new byte[] { (byte) value });
        } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return Unpooled.buffer(Short.BYTES).writeShortLE((short) value);
        } else if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return Unpooled.buffer(Integer.BYTES).writeIntLE((int) value);
        } else {
            return Unpooled.buffer(Long.BYTES).writeLongLE(value);
        }
    }
}
