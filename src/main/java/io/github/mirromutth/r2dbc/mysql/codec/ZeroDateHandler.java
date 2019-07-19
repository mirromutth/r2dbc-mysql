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

import io.github.mirromutth.r2dbc.mysql.constant.SqlStates;
import io.github.mirromutth.r2dbc.mysql.constant.ZeroDateOption;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;

/**
 * An utility for handle "zero date" general logic.
 */
final class ZeroDateHandler {

    private ZeroDateHandler() {
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
}
