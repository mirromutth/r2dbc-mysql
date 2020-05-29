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
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

/**
 * Unit tests for {@link LocalTimeCodec}.
 */
class LocalTimeCodecTest implements CodecTestSupport<LocalTime> {

    static final LocalTime[] TIMES = {
        LocalTime.MIDNIGHT,
        LocalTime.NOON,
        LocalTime.MAX,
        LocalTime.of(11, 22, 33, 1000),
        LocalTime.of(11, 22, 33, 200000),
        LocalTime.of(12, 34, 56, 789100000),
        LocalTime.of(9, 8, 7, 654321000),
    };

    private final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
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
    public LocalTimeCodec getCodec() {
        return LocalTimeCodec.INSTANCE;
    }

    @Override
    public LocalTime[] originParameters() {
        return TIMES;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(TIMES).map(formatter::format).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(TIMES)
            .map(it -> {
                if (LocalTime.MIDNIGHT.equals(it)) {
                    return Unpooled.buffer(0, 0);
                }

                ByteBuf buf = Unpooled.buffer().writeBoolean(false)
                    .writeIntLE(0)
                    .writeByte(it.getHour())
                    .writeByte(it.getMinute())
                    .writeByte(it.getSecond());

                if (it.getNano() != 0) {
                    buf.writeIntLE((int) TimeUnit.NANOSECONDS.toMicros(it.getNano()));
                }

                return buf;
            })
            .toArray(ByteBuf[]::new);
    }
}
