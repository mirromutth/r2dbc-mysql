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

import dev.miku.r2dbc.mysql.ParameterOutputStream;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.ColumnDefinitions;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.Parameter;
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
    public Short decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        if (binary) {
            boolean isUnsigned = (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0;

            switch (info.getType()) {
                case DataTypes.SMALLINT: // Already check overflow in `doCanDecode`
                case DataTypes.YEAR:
                    return value.readShortLE();
                default: // TINYINT
                    if (isUnsigned) {
                        return value.readUnsignedByte();
                    } else {
                        return (short) value.readByte();
                    }
            }
        } else {
            return (short) IntegerCodec.parse(value);
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Short;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        short v = (Short) value;

        if ((byte) v == v) {
            return new ByteCodec.ByteParameter((byte) v);
        }

        return new ShortParameter(v);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        short type = info.getType();

        if (DataTypes.TINYINT == type || DataTypes.YEAR == type) {
            // Note: MySQL not support negative integer for year.
            return true;
        }

        return DataTypes.SMALLINT == type && (info.getDefinitions() & ColumnDefinitions.UNSIGNED) == 0;
    }

    static final class ShortParameter extends AbstractParameter {

        private final short value;

        ShortParameter(short value) {
            this.value = value;
        }

        @Override
        public Mono<Void> binary(ParameterOutputStream output) {
            return Mono.fromRunnable(() -> output.writeShort(value));
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeInt(value));
        }

        @Override
        public short getType() {
            return DataTypes.SMALLINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ShortParameter)) {
                return false;
            }

            ShortParameter that = (ShortParameter) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
