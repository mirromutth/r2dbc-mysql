/*
 * Copyright 2018-2019 the original author or authors.
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

import dev.miku.r2dbc.mysql.message.NormalFieldValue;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.Locale;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Unit tests for {@link LocalDateTimeCodec}.
 */
class LocalDateTimeCodecTest implements CodecTestSupport<LocalDateTime, NormalFieldValue, Class<? super LocalDateTime>> {

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
        String[] results = new String[dateTimes.length];
        for (int i = 0; i < results.length; ++i) {
            results[i] = formatter.format(dateTimes[i]);
        }
        return results;
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
