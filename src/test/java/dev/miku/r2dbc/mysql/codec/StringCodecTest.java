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
 * Unit tests for {@link StringCodec}.
 */
class StringCodecTest implements CodecTestSupport<String> {

    static final String BIG;

    static {
        // Big string with bigger bytes.
        BIG = new String(new char[1024]).replace('\0', '好');
    }

    private final String[] strings = {
        "",
        "Hello, world!",
        "Hi, R2DBC MySQL!",
        "\r\n\0\032\\'\"\u00a5\u20a9",
        "Hello, 简体中文!",
        "Hello, 正體中文!",
        "Hello, 日本語（にほんご）!",
        "Hello, 한국!",
        "Hello, русский!",
        BIG,
    };

    @Override
    public StringCodec getCodec(ByteBufAllocator allocator) {
        return new StringCodec(allocator);
    }

    @Override
    public String[] originParameters() {
        return strings;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(strings)
            .map(string -> String.format("'%s'", ESCAPER.escape(string)))
            .toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(strings)
            .map(it -> Unpooled.wrappedBuffer(it.getBytes(charset)))
            .toArray(ByteBuf[]::new);
    }
}
