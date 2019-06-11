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

import java.util.EnumSet;
import java.util.Set;

/**
 * Codec for {@link int}.
 */
final class IntegerCodec extends AbstractPrimitiveCodec<Integer> {

    private static final Set<DataType> LESS_TYPES = EnumSet.of(
        DataType.TINYINT,
        DataType.YEAR,
        DataType.SMALLINT,
        DataType.MEDIUMINT
    );

    static final IntegerCodec INSTANCE = new IntegerCodec();

    private IntegerCodec() {
        super(Integer.TYPE, Integer.class);
    }

    @Override
    public Integer decodeText(NormalFieldValue value, FieldInformation info, Class<? super Integer> target, MySqlSession session) {
        return parse(value.getBuffer());
    }

    @Override
    public Integer decodeBinary(NormalFieldValue value, FieldInformation info, Class<? super Integer> target, MySqlSession session) {
        ByteBuf buf = value.getBuffer();
        boolean isUnsigned = (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0;

        switch (info.getType()) {
            case INT: // Already check overflow in `doCanDecode`
            case MEDIUMINT:
                return buf.readIntLE();
            case SMALLINT:
                if (isUnsigned) {
                    return buf.readUnsignedShortLE();
                } else {
                    return (int) buf.readShortLE();
                }
            case YEAR:
                return (int) buf.readShortLE();
            default: // TINYINT
                if (isUnsigned) {
                    return (int) buf.readUnsignedByte();
                } else {
                    return (int)  buf.readByte();
                }
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Integer;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return encodeOfInt((Integer) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        DataType type = info.getType();

        if (LESS_TYPES.contains(type)) {
            return true;
        }

        return DataType.INT == type && (info.getDefinitions() & ColumnDefinitions.UNSIGNED) == 0;
    }

    /**
     * Fast parse a negotiable integer from {@link ByteBuf} without copy.
     *
     * @param buf a {@link ByteBuf} include an integer that maybe has sign.
     * @return an integer from {@code buf}.
     */
    static int parse(ByteBuf buf) {
        int value = 0;
        int first = buf.readByte();
        final boolean isNegative;

        if (first == '-') {
            isNegative = true;
        } else if (first >= '0' && first <= '9') {
            isNegative = false;
            value = first - '0';
        } else {
            // must be '+'
            isNegative = false;
        }

        while (buf.isReadable()) {
            value = value * 10 + (buf.readByte() - '0');
        }

        return isNegative ? -value : value;
    }

    static ParameterValue encodeOfInt(int value) {
        return new IntValue(value);
    }

    private static final class IntValue extends AbstractParameterValue {

        private final int value;

        private IntValue(int value) {
            this.value = value;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeInt(value));
        }

        @Override
        public int getNativeType() {
            return DataType.INT.getType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IntValue)) {
                return false;
            }

            IntValue intValue = (IntValue) o;

            return value == intValue.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
