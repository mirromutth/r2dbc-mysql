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

package io.github.mirromutth.r2dbc.mysql.converter;

import io.github.mirromutth.r2dbc.mysql.constant.SqlStates;
import io.github.mirromutth.r2dbc.mysql.constant.ZeroDateOption;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.function.Supplier;

/**
 * A helper for reading java 8 time library from {@link ByteBuf}.
 */
final class JavaTimeHelper {

    private JavaTimeHelper() {
    }

    @Nullable
    static LocalDate readDate(ByteBuf buf) {
        int year = readIntInDigits(buf);
        int month = readIntInDigits(buf);
        int day = readIntInDigits(buf);

        if (month == 0 || day == 0) {
            return null;
        }

        return LocalDate.of(year, month, day);
    }

    static LocalTime readTime(ByteBuf buf) {
        int hour = readIntInDigits(buf);
        int minute = readIntInDigits(buf);
        int second = readIntInDigits(buf);

        // Time should always valid, no need check before construct.
        return LocalTime.of(hour, minute, second);
    }

    @Nullable
    static LocalDateTime readDateTime(ByteBuf buf) {
        LocalDate date = readDate(buf);

        if (date == null) {
            return null;
        }

        LocalTime time = readTime(buf);
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
    static <T extends Temporal> T processZero(ByteBuf buf, ZeroDateOption option, Supplier<T> round) {
        switch (option) {
            case USE_NULL:
                return null;
            case USE_ROUND:
                return round.get();
        }
        String message = String.format("Value '%s' invalid and ZeroDateOption is %s", buf.toString(StandardCharsets.US_ASCII), ZeroDateOption.EXCEPTION.name());
        throw new R2dbcNonTransientResourceException(message, SqlStates.ILLEGAL_ARGUMENT);
    }
}
