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
import io.github.mirromutth.r2dbc.mysql.internal.LazyLoad;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;
import java.time.LocalDate;

/**
 * Converter for {@link LocalDate}.
 */
final class LocalDateConverter implements Converter<LocalDate, Class<LocalDate>> {

    static final LazyLoad<LocalDate> ROUND = LazyLoad.of(() -> LocalDate.of(1, 1, 1));

    static final LocalDateConverter INSTANCE = new LocalDateConverter();

    private LocalDateConverter() {
    }

    @Override
    public LocalDate read(ByteBuf buf, short definitions, int precision, int collationId, Class<LocalDate> target, MySqlSession session) {
        int readerIndex = buf.readerIndex();
        LocalDate date = JavaTimeHelper.readDate(buf);

        if (date == null) {
            buf.readerIndex(readerIndex); // Reset reader index for read a string for whole buffer.
            return JavaTimeHelper.processZero(buf, session.getZeroDate(), ROUND);
        }

        return date;
    }

    @Override
    public boolean canRead(ColumnType type, short definitions, int precision, int collationId, Type target, MySqlSession session) {
        return ColumnType.DATE == type && LocalDate.class == target;
    }
}
