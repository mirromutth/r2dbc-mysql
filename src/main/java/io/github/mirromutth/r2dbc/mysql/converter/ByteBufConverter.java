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

/**
 * Converter for {@link ByteBuf}.
 */
final class ByteBufConverter extends AbstractClassedConverter<ByteBuf> {

    static final ByteBufConverter INSTANCE = new ByteBufConverter();

    private ByteBufConverter() {
        super(ByteBuf.class);
    }

    @Override
    public ByteBuf read(ByteBuf buf, boolean isUnsigned, int precision, int collationId, Class<? super ByteBuf> target, MySqlSession session) {
        return buf.copy();
    }

    @Override
    boolean doCanRead(ColumnType type, boolean isUnsigned) {
        return true; // any type can be byte array
    }
}
