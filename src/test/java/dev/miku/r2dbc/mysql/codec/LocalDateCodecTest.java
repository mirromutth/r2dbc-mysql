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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.Arrays;
import java.util.Locale;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Unit tests for {@link LocalDateCodec}.
 */
class LocalDateCodecTest implements CodecTestSupport<LocalDate> {

    static final LocalDate[] DATES = {
        LocalDate.of(0, 1, 1),
        LocalDate.of(2012, 12, 21),
        LocalDate.of(10, 11, 12),
        LocalDate.of(654, 3, 21),
        // Following should not be permitted by MySQL server, but also test.
        LocalDate.of(-46, 11, 12),
        LocalDate.MIN,
        LocalDate.MAX,
    };

    private final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
        .appendLiteral('\'')
        .appendValue(YEAR, 4, 19, SignStyle.NORMAL)
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 2)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 2)
        .appendLiteral('\'')
        .toFormatter(Locale.ENGLISH);

    @Override
    public LocalDateCodec getCodec(ByteBufAllocator allocator) {
        return new LocalDateCodec(allocator);
    }

    @Override
    public LocalDate[] originParameters() {
        return DATES;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(DATES).map(formatter::format).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(DATES)
            .map(LocalDateCodecTest::encode)
            .toArray(ByteBuf[]::new);
    }

    static ByteBuf encode(LocalDate date) {
        return Unpooled.buffer().writeShortLE(date.getYear())
            .writeByte(date.getMonthValue())
            .writeByte(date.getDayOfMonth());
    }
}
