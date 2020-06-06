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
 * Unit tests for {@link BooleanCodec}.
 */
class BooleanCodecTest implements CodecTestSupport<Boolean> {

    private final Boolean[] booleans = {
        Boolean.TRUE,
        Boolean.FALSE
    };

    @Override
    public BooleanCodec getCodec(ByteBufAllocator allocator) {
        return new BooleanCodec(allocator);
    }

    @Override
    public Boolean[] originParameters() {
        return booleans;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(booleans).map(it -> it ? "b'1'" : "b'0'").toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(booleans)
            .map(it -> Unpooled.wrappedBuffer(it ? new byte[] {1} : new byte[] {0}))
            .toArray(ByteBuf[]::new);
    }

    @Override
    public ByteBuf sized(ByteBuf value) {
        return value;
    }
}
