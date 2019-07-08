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

package io.github.mirromutth.r2dbc.mysql.codec;

import io.github.mirromutth.r2dbc.mysql.constant.BinaryDateTimes;
import io.github.mirromutth.r2dbc.mysql.constant.SqlStates;
import io.github.mirromutth.r2dbc.mysql.constant.ZeroDateOption;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An utility for fast decode java 8 date/time from {@link ByteBuf}.
 */
final class JavaTimeHelper {

    private JavaTimeHelper() {
    }

    static Duration readDurationBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < BinaryDateTimes.TIME_SIZE) {
            return Duration.ZERO;
        }

        boolean isNegative = buf.readBoolean();

        long day = buf.readUnsignedIntLE();
        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();
        long totalSeconds = TimeUnit.DAYS.toSeconds(day) +
            TimeUnit.HOURS.toSeconds(hour) +
            TimeUnit.MINUTES.toSeconds(minute) +
            second;

        if (bytes < BinaryDateTimes.MICRO_TIME_SIZE) {
            return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds);
        }

        long nanos = TimeUnit.MICROSECONDS.toNanos(buf.readUnsignedIntLE());

        return Duration.ofSeconds(isNegative ? -totalSeconds : totalSeconds, isNegative ? -nanos : nanos);
    }

    @Nullable
    static TemporalAccessor readDateTimeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < BinaryDateTimes.DATE_SIZE) {
            return null;
        }

        short year = buf.readShortLE();
        byte month = buf.readByte();
        byte day = buf.readByte();

        if (month == 0 || day == 0) {
            return null;
        }

        if (bytes < BinaryDateTimes.DATETIME_SIZE) {
            return LocalDate.of(year, month, day);
        }

        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();

        if (bytes < BinaryDateTimes.MICRO_DATETIME_SIZE) {
            return LocalDateTime.of(year, month, day, hour, minute, second);
        }

        long micros = buf.readUnsignedIntLE();

        return LocalDateTime.of(year, month, day, hour, minute, second, (int) TimeUnit.MICROSECONDS.toNanos(micros));
    }

    static LocalTime readTimeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < BinaryDateTimes.TIME_SIZE) {
            return LocalTime.MIDNIGHT;
        }

        // Skip sign and day.
        buf.skipBytes(Byte.BYTES + Integer.BYTES);

        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();

        if (bytes < BinaryDateTimes.MICRO_TIME_SIZE) {
            return LocalTime.of(hour, minute, second);
        }

        long micros = buf.readUnsignedIntLE();

        return LocalTime.of(hour, minute, second, (int) TimeUnit.MICROSECONDS.toNanos(micros));
    }

    @Nullable
    static LocalDate readDateText(ByteBuf buf) {
        int year = readIntInDigits(buf);
        int month = readIntInDigits(buf);
        int day = readIntInDigits(buf);

        if (month == 0 || day == 0) {
            return null;
        }

        return LocalDate.of(year, month, day);
    }

    static Duration readDurationText(ByteBuf buf) {
        int hour = readIntInDigits(buf);
        int minute = readIntInDigits(buf);
        int second = readIntInDigits(buf);
        long totalSeconds = TimeUnit.HOURS.toSeconds(hour) + TimeUnit.MINUTES.toSeconds(minute) + second;

        return Duration.ofSeconds(totalSeconds);
    }

    static LocalTime readTimeText(ByteBuf buf) {
        int hour = readIntInDigits(buf);
        int minute = readIntInDigits(buf);
        int second = readIntInDigits(buf);

        // Time should always valid, no need check before construct.
        return LocalTime.of(hour, minute, second);
    }

    @Nullable
    static LocalDateTime readDateTimeText(ByteBuf buf) {
        LocalDate date = readDateText(buf);

        if (date == null) {
            return null;
        }

        LocalTime time = readTimeText(buf);
        return LocalDateTime.of(date, time);
    }

    private static int readIntInDigits(ByteBuf buf) {
        int result = 0;
        int digit;

        while (buf.isReadable()) {
            digit = buf.readByte();

            if (digit >= '0' && digit <= '9') {
                result = result * 10 + (digit - '0');
            } else {
                // Is not digit, means parse completed.
                return result;
            }
        }

        return result;
    }

    @Nullable
    static <T extends Temporal> T processZero(ZeroDateOption option, Supplier<T> round, Supplier<String> value) {
        switch (option) {
            case USE_NULL:
                return null;
            case USE_ROUND:
                return round.get();
        }
        String message = String.format("Value '%s' invalid and ZeroDateOption is %s", value.get(), ZeroDateOption.EXCEPTION.name());
        throw new R2dbcNonTransientResourceException(message, SqlStates.ILLEGAL_ARGUMENT);
    }
}
