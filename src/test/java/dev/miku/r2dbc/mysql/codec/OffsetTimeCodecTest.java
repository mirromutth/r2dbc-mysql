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
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Arrays;

/**
 * Unit tests for {@link OffsetTimeCodec}.
 */
class OffsetTimeCodecTest extends TimeCodecTestSupport<OffsetTime> {

    private final OffsetTime[] times;

    OffsetTimeCodecTest() {
        OffsetTime[] times = new OffsetTime[LocalTimeCodecTest.TIMES.length << 2];

        for (int i = 0; i < LocalTimeCodecTest.TIMES.length; ++i) {
            LocalTime time = LocalTimeCodecTest.TIMES[i];

            times[i << 2] = OffsetTime.of(time, ZoneOffset.MIN);
            times[(i << 2) + 1] = OffsetTime.of(time, ZoneOffset.of("+6"));
            times[(i << 2) + 2] = OffsetTime.of(time, ZoneOffset.of("+10"));
            times[(i << 2) + 3] = OffsetTime.of(time, ZoneOffset.MAX);
        }

        this.times = times;
    }

    @Override
    public OffsetTimeCodec getCodec(ByteBufAllocator allocator) {
        return new OffsetTimeCodec(allocator);
    }

    @Override
    public OffsetTime[] originParameters() {
        return times;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(times).map(this::convert).map(this::toText).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(times).map(this::convert).map(this::toBinary).toArray(ByteBuf[]::new);
    }

    private LocalTime convert(OffsetTime value) {
        return value.withOffsetSameInstant((ZoneOffset) ENCODE_SERVER_ZONE).toLocalTime();
    }
}
