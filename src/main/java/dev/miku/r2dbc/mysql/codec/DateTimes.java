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

import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.time.LocalDate;
import java.time.temporal.Temporal;

/**
 * An utility considers date/time generic logic for {@link Codec} implementations.
 */
final class DateTimes {

    static final int DATE_SIZE = Short.BYTES + (Byte.BYTES << 1);

    static final int DATETIME_SIZE = DATE_SIZE + (Byte.BYTES * 3);

    static final int MICRO_DATETIME_SIZE = DATETIME_SIZE + Integer.BYTES;

    static final int TIME_SIZE = Byte.BYTES + Integer.BYTES + (Byte.BYTES * 3);

    static final int MICRO_TIME_SIZE = TIME_SIZE + Integer.BYTES;

    static final int HOURS_OF_DAY = 24;

    static final int SECONDS_OF_MINUTE = 60;

    static final int SECONDS_OF_HOUR = SECONDS_OF_MINUTE * 60;

    static final int SECONDS_OF_DAY = SECONDS_OF_HOUR * HOURS_OF_DAY;

    static final int NANOS_OF_SECOND = 1000_000_000;

    static final int NANOS_OF_MICRO = 1000;

    private static final String ILLEGAL_ARGUMENT = "S1009";

    private static final int MICRO_DIGITS = 6;

    /**
     * Read microseconds part, it is not like {@link #readIntInDigits(ByteBuf)}。
     * For example, 3:45:59.1, should format microseconds as 100000 rather than 1.
     *
     * @param buf the buffer that want to be decoded.
     * @return the value of microseconds, from 0 to 999999.
     */
    static int readMicroInDigits(ByteBuf buf) {
        if (!buf.isReadable()) {
            return 0;
        }

        int micro = 0;
        int num;
        byte digit;
        for (num = MICRO_DIGITS; buf.isReadable() && num > 0; --num) {
            digit = buf.readByte();

            if (digit < '0' || digit > '9') {
                break;
            }

            micro = micro * 10 + (digit - '0');
        }

        while (num-- > 0) {
            micro *= 10;
        }

        return micro;
    }

    static int readIntInDigits(ByteBuf buf) {
        if (!buf.isReadable()) {
            return 0;
        }

        int writerIndex = buf.writerIndex();
        int result = 0;
        byte digit;

        for (int i = buf.readerIndex(); i < writerIndex; ++i) {
            digit = buf.getByte(i);

            if (digit >= '0' && digit <= '9') {
                result = result * 10 + (digit - '0');
            } else {
                buf.readerIndex(i + 1);
                // Is not digit, means parse completed.
                return result;
            }
        }

        // Parse until end-of-buffer.
        buf.readerIndex(writerIndex);
        return result;
    }

    @Nullable
    static <T extends Temporal> T zeroDate(ZeroDateOption option, boolean binary, T round) {
        switch (option) {
            case USE_NULL:
                return null;
            case USE_ROUND:
                return round;
        }

        String message = (binary ? "Binary" : "Text") + " value is zero date and ZeroDateOption is " + ZeroDateOption.EXCEPTION;
        throw new R2dbcNonTransientResourceException(message, ILLEGAL_ARGUMENT);
    }

    static boolean canDecodeChronology(short type, ParameterizedType target, Class<? extends Temporal> chronology) {
        return (DataTypes.DATETIME == type || DataTypes.TIMESTAMP == type || DataTypes.TIMESTAMP2 == type) &&
            LocalDate.class == ParametrizedUtils.getTypeArgument(target, chronology);
    }

    static boolean canDecodeDateTime(short type, Class<?> target, Class<? extends Temporal> temporal) {
        return (DataTypes.DATETIME == type || DataTypes.TIMESTAMP == type || DataTypes.TIMESTAMP2 == type) &&
            target.isAssignableFrom(temporal);
    }

    private DateTimes() {
    }
}
