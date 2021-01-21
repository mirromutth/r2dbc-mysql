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
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Unit tests for {@link ClobCodec}.
 */
class ClobCodecTest implements CodecTestSupport<Clob> {

    private final MockClob[] clob = {
        new MockClob(),
        new MockClob(""),
        new MockClob("", ""),
        new MockClob("Hello, world!"),
        new MockClob("\r\n\0\032\\'\"\u00a5\u20a9"),
        new MockClob("Hello, ", "R2DBC", "!"),
        new MockClob("Hello, ", "简体中文", "!"),
        new MockClob("Hello, ", "正體中文", "!"),
        new MockClob("Hello, ", "日本語（にほんご）", "!"),
        new MockClob("Hello, ", "한국", "!"),
        new MockClob("Hello, ", "русский", "!"),
        new MockClob("", "Hi, ", "MySQL"),
        new MockClob("Hi, ", "", "MySQL"),
        new MockClob("Hi, ", "MySQL", ""),
        new MockClob(StringCodecTest.BIG, StringCodecTest.BIG, StringCodecTest.BIG)
    };

    @Override
    public ClobCodec getCodec(ByteBufAllocator allocator) {
        return new ClobCodec(allocator);
    }

    @Override
    public Clob[] originParameters() {
        return clob;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(clob)
            .map(it -> String.format("'%s'", ESCAPER.escape(String.join("", it.values))))
            .toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(clob)
            .map(it -> Unpooled.wrappedBuffer(String.join("", it.values).getBytes(charset)))
            .toArray(ByteBuf[]::new);
    }

    private static final class MockClob implements Clob {

        private final String[] values;

        private MockClob(String... values) {
            this.values = values;
        }

        @Override
        public Flux<CharSequence> stream() {
            return Flux.fromArray(values);
        }

        @Override
        public Mono<Void> discard() {
            return Mono.empty();
        }
    }
}
