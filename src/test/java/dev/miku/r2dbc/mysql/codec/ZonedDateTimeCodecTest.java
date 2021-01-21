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
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ZonedDateTimeCodec}.
 */
class ZonedDateTimeCodecTest extends DateTimeCodecTestSupport<ZonedDateTime> {

    private static final ZonedDateTime NY_ST = ZonedDateTime.of(2019, 1, 1,
        11, 59, 59, 1234_000, ZoneId.of("America/New_York"));

    private static final ZonedDateTime NY_DST = NY_ST.plusHours(200 * 24);

    private final ZonedDateTime[] dateTimes = {
        ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC),
        NY_ST,
        NY_DST,
        ZonedDateTime.of(2020, 6, 1, 12, 0, 0, 56789000, ZoneOffset.of("+10"))
    };

    @Test
    void daylightTime() {
        assertThat(NY_ST.toLocalTime().plusHours(1))
            .isEqualTo(NY_DST.toLocalTime());
        assertThat(convert(NY_ST).toLocalTime())
            .isEqualTo(convert(NY_DST).toLocalTime());
    }

    @Override
    public Codec<ZonedDateTime> getCodec(ByteBufAllocator allocator) {
        return new ZonedDateTimeCodec(allocator);
    }

    @Override
    public ZonedDateTime[] originParameters() {
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

    private LocalDateTime convert(ZonedDateTime value) {
        return value.withZoneSameInstant(ENCODE_SERVER_ZONE).toLocalDateTime();
    }
}
