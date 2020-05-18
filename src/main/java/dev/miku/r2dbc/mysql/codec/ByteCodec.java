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

import dev.miku.r2dbc.mysql.constant.ColumnDefinitions;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;

/**
 * Codec for {@link byte}.
 */
final class ByteCodec extends AbstractPrimitiveCodec<Byte> {

    static final ByteCodec INSTANCE = new ByteCodec();

    private ByteCodec() {
        super(Byte.TYPE, Byte.class);
    }

    @Override
    public Byte decode(ByteBuf value, FieldInformation info, Type target, boolean binary, ConnectionContext context) {
        if (binary) {
            return value.readByte();
        } else {
            return (byte) IntegerCodec.parse(value);
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Byte;
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new ByteValue((Byte) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return DataTypes.TINYINT == info.getType() && (info.getDefinitions() & ColumnDefinitions.UNSIGNED) == 0;
    }

    static final class ByteValue extends AbstractParameterValue {

        private final byte value;

        ByteValue(byte value) {
            this.value = value;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeByte(value));
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.fromRunnable(() -> builder.append(value));
        }

        @Override
        public short getType() {
            return DataTypes.TINYINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ByteValue)) {
                return false;
            }

            ByteValue byteValue = (ByteValue) o;

            return value == byteValue.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
