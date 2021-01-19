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

import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * Codec for {@code double}.
 */
final class DoubleCodec extends AbstractPrimitiveCodec<Double> {

    DoubleCodec(ByteBufAllocator allocator) {
        super(allocator, Double.TYPE, Double.class);
    }

    @Override
    public Double decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary,
        CodecContext context) {
        if (binary) {
            switch (info.getType()) {
                case DataTypes.DOUBLE:
                    return value.readDoubleLE();
                case DataTypes.FLOAT:
                    return (double) value.readFloatLE();
            }
            // DECIMAL and size less than 16, encoded by text.
        }
        return Double.parseDouble(value.toString(StandardCharsets.US_ASCII));
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Double;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new DoubleParameter(allocator, (Double) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        short type = info.getType();
        return DataTypes.DOUBLE == type || DataTypes.FLOAT == type ||
            (info.getSize() < 16 && TypePredicates.isDecimal(type));
    }

    private static final class DoubleParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final double value;

        private DoubleParameter(ByteBufAllocator allocator, double value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> {
                ByteBuf buf = allocator.buffer(Double.BYTES);
                try {
                    return buf.writeDoubleLE(value);
                } catch (Throwable e) {
                    buf.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeDouble(value));
        }

        @Override
        public short getType() {
            return DataTypes.DOUBLE;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DoubleParameter)) {
                return false;
            }

            DoubleParameter that = (DoubleParameter) o;

            return Double.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            long temp = Double.doubleToLongBits(value);
            return (int) (temp ^ (temp >>> 32));
        }
    }
}
