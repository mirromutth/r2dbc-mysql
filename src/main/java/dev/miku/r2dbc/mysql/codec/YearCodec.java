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

import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;
import java.time.Year;

/**
 * Codec for {@link Year}.
 * <p>
 * Note: unsupported YEAR(2) because it is deprecated feature in MySQL 5.x.
 */
final class YearCodec extends AbstractClassedCodec<Year> {

    static final YearCodec INSTANCE = new YearCodec();

    private YearCodec() {
        super(Year.class);
    }

    @Override
    public Year decode(ByteBuf value, FieldInformation info, Type target, boolean binary, ConnectionContext context) {
        if (binary) {
            return Year.of(value.readShortLE());
        } else {
            return Year.of(IntegerCodec.parse(value));
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Year;
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        int year = ((Year) value).getValue();

        if ((byte) year == year) {
            return new ByteCodec.ByteValue((byte) year);
        }

        if ((short) year == year) {
            return new ShortCodec.ShortValue((short) year);
        }

        // Unsupported, but should be considered here.
        return new IntegerCodec.IntValue(year);
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataTypes.YEAR == info.getType();
    }
}
