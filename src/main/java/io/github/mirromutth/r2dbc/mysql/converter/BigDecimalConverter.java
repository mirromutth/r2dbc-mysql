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

import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.Set;

/**
 * Converter for {@link BigDecimal}.
 */
final class BigDecimalConverter extends AbstractClassedConverter<BigDecimal> {

    static final Set<ColumnType> DECIMAL_TYPES = EnumSet.of(ColumnType.DECIMAL, ColumnType.NEW_DECIMAL);

    static final BigDecimalConverter INSTANCE = new BigDecimalConverter();

    private BigDecimalConverter() {
        super(BigDecimal.class);
    }

    @Override
    public BigDecimal read(ByteBuf buf, boolean isUnsigned, int precision, int collationId, Class<? super BigDecimal> target, MySqlSession session) {
        // TODO: implement this method
        throw new IllegalStateException();
    }

    @Override
    boolean doCanRead(ColumnType type, boolean isUnsigned) {
        return DECIMAL_TYPES.contains(type);
    }
}
