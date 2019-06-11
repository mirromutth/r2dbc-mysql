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

import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * Codec for {@link double}.
 */
final class DoubleCodec extends AbstractPrimitiveCodec<Double> {

    static final DoubleCodec INSTANCE = new DoubleCodec();

    private DoubleCodec() {
        super(Double.TYPE, Double.class);
    }

    @Override
    public Double decodeText(NormalFieldValue value, FieldInformation info, Class<? super Double> target, MySqlSession session) {
        return Double.parseDouble(value.getBuffer().toString(StandardCharsets.US_ASCII));
    }

    @Override
    public Double decodeBinary(NormalFieldValue value, FieldInformation info, Class<? super Double> target, MySqlSession session) {
        ByteBuf buf = value.getBuffer();

        switch (info.getType()) {
            case DOUBLE:
                return buf.readDoubleLE();
            case FLOAT:
                return (double) buf.readFloatLE();
            default: // DECIMAL and size less than 16
                return Double.parseDouble(buf.toString(StandardCharsets.US_ASCII));
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Double;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new DoubleValue((Double) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        DataType type = info.getType();
        return DataType.DOUBLE == type || DataType.FLOAT == type || (info.getSize() < 16 && TypeConditions.isDecimal(type));
    }

    private static final class DoubleValue extends AbstractParameterValue {

        private final double value;

        private DoubleValue(double value) {
            this.value = value;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeDouble(value));
        }

        @Override
        public int getNativeType() {
            return DataType.DOUBLE.getType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DoubleValue)) {
                return false;
            }

            DoubleValue that = (DoubleValue) o;

            return Double.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            long temp = Double.doubleToLongBits(value);
            return (int) (temp ^ (temp >>> 32));
        }
    }
}
