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

import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
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

    static final int SECONDS_OF_MINUTE = 60;

    static final int SECONDS_OF_HOUR = SECONDS_OF_MINUTE * 60;

    static final int SECONDS_OF_DAY = SECONDS_OF_HOUR * 24;

    static final int NANOS_OF_SECOND = 1000_000_000;

    static final int NANOS_OF_MICRO = 1000;

    private static final String ILLEGAL_ARGUMENT = "S1009";

    static int readIntInDigits(ByteBuf buf) {
        if (!buf.isReadable()) {
            return 0;
        }

        int readerIndex = buf.readerIndex();
        int writerIndex = buf.writerIndex();
        int result = 0;
        byte digit;

        for (int i = readerIndex; i < writerIndex; ++i) {
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
    static <T extends Temporal> T zeroDate(ZeroDateOption option, boolean binary, ByteBuf buf, int index, int bytes, T round) {
        switch (option) {
            case USE_NULL:
                return null;
            case USE_ROUND:
                return round;
        }

        String message;
        if (binary) {
            message = String.format("Binary value %s (hex dump) invalid and ZeroDateOption is %s", ByteBufUtil.hexDump(buf, index, bytes), ZeroDateOption.EXCEPTION.name());
        } else {
            message = String.format("Text value '%s' invalid and ZeroDateOption is %s", buf.toString(StandardCharsets.US_ASCII), ZeroDateOption.EXCEPTION.name());
        }

        throw new R2dbcNonTransientResourceException(message, ILLEGAL_ARGUMENT);
    }

    private DateTimes() {
    }
}
