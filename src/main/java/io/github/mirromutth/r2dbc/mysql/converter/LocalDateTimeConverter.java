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

import io.github.mirromutth.r2dbc.mysql.constant.ColumnType;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Converter for {@link LocalDateTime}.
 */
final class LocalDateTimeConverter implements Converter<LocalDateTime, Class<LocalDateTime>> {

    static final LocalDateTimeConverter INSTANCE = new LocalDateTimeConverter();

    private LocalDateTimeConverter() {
    }

    @Override
    public LocalDateTime read(ByteBuf buf, short definitions, int precision, int collationId, Class<LocalDateTime> target, MySqlSession session) {
        int readerIndex = buf.readerIndex();
        LocalDateTime dateTime = JavaTimeHelper.readDateTime(buf);

        if (dateTime == null) {
            buf.readerIndex(readerIndex); // Reset reader index for read a string for whole buffer.
            return JavaTimeHelper.processZero(buf, session.getZeroDateOption(), Holder::getRound);
        }

        return dateTime;
    }

    @Override
    public boolean canRead(ColumnType type, short definitions, int precision, int collationId, Type target, MySqlSession session) {
        return (ColumnType.DATETIME == type || ColumnType.TIMESTAMP == type || ColumnType.TIMESTAMP2 == type) && LocalDateTime.class == target;
    }

    private static final class Holder {

        private static final LocalDateTime ROUND = LocalDateTime.of(LocalDateConverter.Holder.getRound(), LocalTime.MIN);

        private static LocalDateTime getRound() {
            return ROUND;
        }
    }
}
