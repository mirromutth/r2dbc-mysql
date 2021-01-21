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

import java.nio.charset.Charset;
import java.time.LocalTime;
import java.util.Arrays;

/**
 * Unit tests for {@link LocalTimeCodec}.
 */
class LocalTimeCodecTest extends TimeCodecTestSupport<LocalTime> {

    static final LocalTime[] TIMES = {
        LocalTime.MIDNIGHT,
        LocalTime.NOON,
        LocalTime.MAX,
        LocalTime.of(11, 22, 33, 1000),
        LocalTime.of(11, 22, 33, 200000),
        LocalTime.of(12, 34, 56, 789100000),
        LocalTime.of(9, 8, 7, 654321000),
    };

    @Override
    public LocalTimeCodec getCodec(ByteBufAllocator allocator) {
        return new LocalTimeCodec(allocator);
    }

    @Override
    public LocalTime[] originParameters() {
        return TIMES;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(TIMES).map(this::toText).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(TIMES).map(this::toBinary).toArray(ByteBuf[]::new);
    }
}
