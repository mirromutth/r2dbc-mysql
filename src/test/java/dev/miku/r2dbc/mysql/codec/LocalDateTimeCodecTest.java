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

import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoField.*;

/**
 * Unit tests for {@link LocalDateTimeCodec}.
 */
class LocalDateTimeCodecTest implements CodecTestSupport<LocalDateTime> {

    private final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
        .appendLiteral('\'')
        .appendValue(YEAR, 4, 19, SignStyle.NORMAL)
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 2)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 2)
        .appendLiteral(' ')
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .optionalStart()
        .appendFraction(MICRO_OF_SECOND, 0, 6, true)
        .optionalEnd()
        .appendLiteral('\'')
        .toFormatter(Locale.ENGLISH);

    private final LocalDateTime[] dateTimes = multiple();

    @Override
    public LocalDateTimeCodec getCodec() {
        return LocalDateTimeCodec.INSTANCE;
    }

    @Override
    public LocalDateTime[] originParameters() {
        return dateTimes;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(dateTimes).map(formatter::format).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(dateTimes)
            .map(it -> {
                ByteBuf buf = LocalDateCodecTest.encode(it.toLocalDate());
                LocalTime time = it.toLocalTime();

                if (LocalTime.MIDNIGHT.equals(time)) {
                    return buf;
                }

                buf.writeByte(time.getHour())
                    .writeByte(time.getMinute())
                    .writeByte(time.getSecond());

                if (time.getNano() != 0) {
                    buf.writeIntLE((int) TimeUnit.NANOSECONDS.toMicros(time.getNano()));
                }

                return buf;
            })
            .toArray(ByteBuf[]::new);
    }

    private static LocalDateTime[] multiple() {
        LocalDateTime[] results = new LocalDateTime[LocalDateCodecTest.DATES.length * LocalTimeCodecTest.TIMES.length];
        for (int i = 0; i < LocalDateCodecTest.DATES.length; ++i) {
            for (int j = 0; j < LocalTimeCodecTest.TIMES.length; ++j) {
                results[i * (LocalTimeCodecTest.TIMES.length) + j] = LocalDateTime.of(LocalDateCodecTest.DATES[i], LocalTimeCodecTest.TIMES[j]);
            }
        }
        return results;
    }
}
