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

import dev.miku.r2dbc.mysql.constant.SqlStates;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;

/**
 * An utility with date/time generic logic for {@link Codec} implementations.
 */
final class CodecDateUtils {

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
    static <T extends Temporal> T handle(ZeroDateOption option, boolean binary, ByteBuf buf, int index, int bytes, T round) {
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

        throw new R2dbcNonTransientResourceException(message, SqlStates.ILLEGAL_ARGUMENT);
    }

    private CodecDateUtils() {
    }
}
