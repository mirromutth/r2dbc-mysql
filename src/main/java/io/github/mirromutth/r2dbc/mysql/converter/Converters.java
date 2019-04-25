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
import io.github.mirromutth.r2dbc.mysql.json.MySqlJson;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Type;

/**
 * Bind all converters for all types.
 */
public interface Converters {

    @Nullable
    <T> T read(@Nullable ByteBuf buf, @Nullable ColumnType columnType, boolean isUnsigned, int precision, int collationId, Type targetType, MySqlSession session);

    static Converters create(MySqlJson mySqlJson) {
        return new DefaultConverters(
            StringConverter.INSTANCE,

            ByteConverter.INSTANCE,
            ShortConverter.INSTANCE,
            IntegerConverter.INSTANCE,
            LongConverter.INSTANCE,
            BigIntegerConverter.INSTANCE,

            FloatConverter.INSTANCE,
            DoubleConverter.INSTANCE,
            BigDecimalConverter.INSTANCE,

            BooleanConverter.INSTANCE,
            BitSetConverter.INSTANCE,

            InstantConverter.INSTANCE,
            LocalDateTimeConverter.INSTANCE,
            ZonedDateTimeConverter.INSTANCE,
            OffsetDateTimeConverter.INSTANCE,

            LocalDateConverter.INSTANCE,
            YearConverter.INSTANCE,

            LocalTimeConverter.INSTANCE,
            OffsetTimeConverter.INSTANCE,

            ByteArrayConverter.INSTANCE,
            ByteBufConverter.INSTANCE,
            BoxedByteArrayConverter.INSTANCE,

            EnumConverter.INSTANCE,
            SetConverter.INSTANCE,

            new JsonConverter(mySqlJson)
        );
    }
}
