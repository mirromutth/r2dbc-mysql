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
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;
import java.time.LocalTime;

/**
 * Converter for {@link LocalTime}.
 * <p>
 * Can not support {@code OffsetTime} because of {@code ZoneId} can not be
 * convert to {@code ZoneOffset} with time only.
 */
final class LocalTimeConverter implements Converter<LocalTime, Class<LocalTime>> {

    static final LocalTimeConverter INSTANCE = new LocalTimeConverter();

    private LocalTimeConverter() {
    }

    @Override
    public LocalTime read(ByteBuf buf, short definitions, int precision, int collationId, Class<LocalTime> target, MySqlSession session) {
        return JavaTimeHelper.readTime(buf);
    }

    @Override
    public boolean canRead(ColumnType type, short definitions, int precision, int collationId, Type target, MySqlSession session) {
        return ColumnType.TIME == type && LocalTime.class == target;
    }
}
