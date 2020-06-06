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
import java.time.Year;
import java.util.Arrays;

/**
 * Unit tests for {@link YearCodec}.
 */
class YearCodecTest implements CodecTestSupport<Year> {

    private final Year[] years = {
        Year.of(0),
        Year.of(63),
        Year.of(127),
        Year.of(255),
        Year.of(1000),
        Year.of(1900),
        Year.of(2100),
        // Following should not be permitted by MySQL server, but also test.
        Year.of(-63),
        Year.of(-127),
        Year.of(-255),
        Year.of(-1000),
        Year.of(-1900),
        Year.of(-2100),
        Year.of(Short.MAX_VALUE),
        Year.of(65535),
        Year.of(Year.MAX_VALUE),
        Year.of(Year.MIN_VALUE),
    };

    @Override
    public YearCodec getCodec(ByteBufAllocator allocator) {
        return new YearCodec(allocator);
    }

    @Override
    public Year[] originParameters() {
        return years;
    }

    @Override
    public Object[] stringifyParameters() {
        return years;
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(years)
            .map(Year::getValue)
            .map(it -> {
                if (it >= Byte.MIN_VALUE && it <= Byte.MAX_VALUE) {
                    return Unpooled.wrappedBuffer(new byte[]{it.byteValue()});
                } else if (it >= Short.MIN_VALUE && it <= Short.MAX_VALUE) {
                    return Unpooled.buffer(Short.BYTES, Short.BYTES).writeShortLE(it.shortValue());
                } else {
                    return Unpooled.buffer(Integer.BYTES, Integer.BYTES).writeIntLE(it);
                }
            })
            .toArray(ByteBuf[]::new);
    }

    @Override
    public ByteBuf sized(ByteBuf value) {
        return value;
    }
}
