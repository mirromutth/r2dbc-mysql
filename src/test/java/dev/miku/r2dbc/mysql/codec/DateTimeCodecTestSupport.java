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

import dev.miku.r2dbc.mysql.ConnectionContextTest;
import io.netty.buffer.ByteBuf;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoField.*;

/**
 * Base class considers codecs unit tests of date/time.
 */
abstract class DateTimeCodecTestSupport<T extends TemporalAccessor> implements CodecTestSupport<T> {

    protected static final ZoneId SERVER_ZONE_ID = ZoneId.of("GMT+6");

    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
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

    static {
        System.setProperty("user.timezone", "GMT+2");
    }

    @Override
    public CodecContext context() {
        return ConnectionContextTest.mock(SERVER_ZONE_ID);
    }

    protected final String toText(TemporalAccessor dateTime) {
        return FORMATTER.format(dateTime);
    }

    protected final ByteBuf toBinary(LocalDateTime dateTime) {
        ByteBuf buf = LocalDateCodecTest.encode(dateTime.toLocalDate());
        LocalTime time = dateTime.toLocalTime();

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
    }
}
