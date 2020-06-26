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

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

/**
 * Unit tests for {@link OffsetDateTimeCodec}.
 */
class OffsetDateTimeCodecTest extends DateTimeCodecTestSupport<OffsetDateTime> {

    private final OffsetDateTime[] dateTimes = {
        OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC),
        OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.MAX),
        OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.MIN),
        OffsetDateTime.of(2020, 6, 1, 12, 0, 0, 56789000, ZoneOffset.of("+10"))
    };

    @Override
    public OffsetDateTimeCodec getCodec(ByteBufAllocator allocator) {
        return new OffsetDateTimeCodec(allocator);
    }

    @Override
    public OffsetDateTime[] originParameters() {
        return dateTimes;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(dateTimes).map(this::convert).map(this::toText).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(dateTimes).map(this::convert).map(this::toBinary).toArray(ByteBuf[]::new);
    }

    private LocalDateTime convert(OffsetDateTime value) {
        return value.withOffsetSameInstant((ZoneOffset) ENCODE_SERVER_ZONE).toLocalDateTime();
    }
}
