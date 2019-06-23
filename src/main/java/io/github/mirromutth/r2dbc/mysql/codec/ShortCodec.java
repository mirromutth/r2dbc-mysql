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

import io.github.mirromutth.r2dbc.mysql.constant.ColumnDefinitions;
import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

/**
 * Codec for {@link short}.
 */
final class ShortCodec extends AbstractPrimitiveCodec<Short> {

    static final ShortCodec INSTANCE = new ShortCodec();

    private ShortCodec() {
        super(Short.TYPE, Short.class);
    }

    @Override
    public Short decodeText(NormalFieldValue value, FieldInformation info, Class<? super Short> target, MySqlSession session) {
        return (short) IntegerCodec.parse(value.getBuffer());
    }

    @Override
    public Short decodeBinary(NormalFieldValue value, FieldInformation info, Class<? super Short> target, MySqlSession session) {
        ByteBuf buf = value.getBuffer();
        boolean isUnsigned = (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0;

        switch (info.getType()) {
            case SMALLINT: // Already check overflow in `doCanDecode`
            case YEAR:
                return buf.readShortLE();
            default: // TINYINT
                if (isUnsigned) {
                    return buf.readUnsignedByte();
                } else {
                    return (short) buf.readByte();
                }
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Short;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new ShortValue((Short) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        DataType type = info.getType();

        if (DataType.TINYINT == type || DataType.YEAR == type) {
            // Note: MySQL not support negative integer for year.
            return true;
        }

        return DataType.SMALLINT == type && (info.getDefinitions() & ColumnDefinitions.UNSIGNED) == 0;
    }

    private static final class ShortValue extends AbstractParameterValue {

        private final short value;

        private ShortValue(short value) {
            this.value = value;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeShort(value));
        }

        @Override
        public int getNativeType() {
            return DataType.SMALLINT.getType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ShortValue)) {
                return false;
            }

            ShortValue that = (ShortValue) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return (int) value;
        }
    }
}
