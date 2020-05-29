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
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.Parameter;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Codec for {@link BigDecimal}.
 */
final class BigDecimalCodec extends AbstractClassedCodec<BigDecimal> {

    static final BigDecimalCodec INSTANCE = new BigDecimalCodec();

    private BigDecimalCodec() {
        super(BigDecimal.class);
    }

    @Override
    public BigDecimal decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        if (binary) {
            short type = info.getType();

            switch (type) {
                case DataTypes.FLOAT:
                    return BigDecimal.valueOf(value.readFloatLE());
                case DataTypes.DOUBLE:
                    return BigDecimal.valueOf(value.readDoubleLE());
            }
            // Not float or double, is text-encoded yet.
        }

        BigDecimal decimal = new BigDecimal(value.toString(StandardCharsets.US_ASCII));

        // Why Java has not BigDecimal.parseBigDecimal(String)?
        if (BigDecimal.ZERO.equals(decimal)) {
            return BigDecimal.ZERO;
        } else if (BigDecimal.ONE.equals(decimal)) {
            return BigDecimal.ONE;
        } else if (BigDecimal.TEN.equals(decimal)) {
            return BigDecimal.TEN;
        } else {
            return decimal;
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof BigDecimal;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new BigDecimalParameter((BigDecimal) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        short type = info.getType();
        return TypePredicates.isDecimal(type) || DataTypes.FLOAT == type || DataTypes.DOUBLE == type;
    }

    private static final class BigDecimalParameter extends AbstractParameter {

        private final BigDecimal value;

        private BigDecimalParameter(BigDecimal value) {
            this.value = value;
        }

        @Override
        public Mono<Void> binary(ParameterOutputStream output) {
            return Mono.fromRunnable(() -> output.writeAsciiString(value.toString()));
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeBigDecimal(value));
        }

        @Override
        public short getType() {
            return DataTypes.NEW_DECIMAL;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BigDecimalParameter)) {
                return false;
            }

            BigDecimalParameter that = (BigDecimalParameter) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
