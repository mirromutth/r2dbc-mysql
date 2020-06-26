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
import io.netty.buffer.Unpooled;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.Temporal;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

/**
 * Base class considers codecs unit tests of time.
 */
abstract class TimeCodecTestSupport<T extends Temporal> implements CodecTestSupport<T> {

    protected static final ZoneId ENCODE_SERVER_ZONE = ZoneId.of("+6");

    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
        .appendLiteral('\'')
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

    @Override
    public CodecContext context() {
        return ConnectionContextTest.mock(ENCODE_SERVER_ZONE);
    }

    protected final String toText(Temporal time) {
        return FORMATTER.format(time);
    }

    protected final ByteBuf toBinary(LocalTime time) {
        if (LocalTime.MIDNIGHT.equals(time)) {
            return Unpooled.buffer(0, 0);
        }

        ByteBuf buf = Unpooled.buffer().writeBoolean(false)
            .writeIntLE(0)
            .writeByte(time.getHour())
            .writeByte(time.getMinute())
            .writeByte(time.getSecond());

        if (time.getNano() != 0) {
            buf.writeIntLE((int) TimeUnit.NANOSECONDS.toMicros(time.getNano()));
        }

        return buf;
    }
}
