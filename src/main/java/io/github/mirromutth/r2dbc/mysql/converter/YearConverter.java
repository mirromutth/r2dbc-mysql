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
import java.time.Year;

/**
 * Converter for {@link Year}.
 * <p>
 * Note: MySQL converts values in the ranges 0 to 69 and 70 to 99 of YEAR(2) to
 * YEAR(4) values in the ranges 2000 to 2069 and 1970 to 1999. (maybe don't need
 * support because this is an deprecated feature?)
 */
final class YearConverter implements Converter<Object, Class<?>> {

    static final YearConverter INSTANCE = new YearConverter();

    private YearConverter() {
    }

    @Override
    public boolean canRead(ColumnType type, short definitions, int precision, int collationId, Type target, MySqlSession session) {
        if (ColumnType.YEAR != type || !(target instanceof Class<?>)) {
            return false;
        }

        Class<?> targetClass = (Class<?>) target;
        return Integer.TYPE == targetClass || targetClass.isAssignableFrom(Integer.class) || targetClass.isAssignableFrom(Year.class);
    }

    @Override
    public Object read(ByteBuf buf, short definitions, int precision, int collationId, Class<?> target, MySqlSession session) {
        int year = processYear(IntegerConverter.parse(buf), precision);

        if (Integer.TYPE == target || target.isAssignableFrom(Integer.class)) {
            return year;
        } else {
            // Must be Year.class
            return Year.of(year);
        }
    }

    private int processYear(int year, int precision) {
        if (precision == 2 && year < 100 && year >= 0) {
            if (year < 70) {
                return 2000 + year;
            } else {
                return 1900 + year;
            }
        } else {
            return year;
        }
    }
}
