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
import org.testcontainers.shaded.com.google.common.base.CaseFormat;
import org.testcontainers.shaded.com.google.common.collect.BoundType;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Unit tests for {@link EnumCodec}.
 */
class EnumCodecTest implements CodecTestSupport<Enum<?>> {

    private final Enum<?>[] enums = {
        // Java has no way to create an element of enum with special character.
        // Maybe imports other languages here?
        SomeElement.A1B2,
        BoundType.OPEN,
        BoundType.CLOSED,
        CaseFormat.LOWER_CAMEL,
        SomeElement.$$$$,
    };

    @Override
    public EnumCodec getCodec(ByteBufAllocator allocator) {
        return new EnumCodec(allocator);
    }

    @Override
    public Enum<?>[] originParameters() {
        return enums;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(enums)
            .map(it -> String.format("'%s'", ESCAPER.escape(it.name())))
            .toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(enums)
            .map(it -> Unpooled.wrappedBuffer(it.name().getBytes(charset)))
            .toArray(ByteBuf[]::new);
    }

    private enum SomeElement {

        $$$$,
        A1B2;

        @Override
        public final String toString() {
            throw new IllegalStateException("Special enum class, can not to string");
        }
    }
}
