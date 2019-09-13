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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import dev.miku.r2dbc.mysql.internal.ConnectionContext;
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
    public BigDecimal decode(NormalFieldValue value, FieldInformation info, Class<? super BigDecimal> target, boolean binary, ConnectionContext context) {
        ByteBuf buf = value.getBufferSlice();

        if (binary) {
            short type = info.getType();

            switch (type) {
                case DataTypes.FLOAT:
                    return BigDecimal.valueOf(buf.readFloatLE());
                case DataTypes.DOUBLE:
                    return BigDecimal.valueOf(buf.readDoubleLE());
            }
            // Not float or double, is text-encoded yet.
        }

        BigDecimal decimal = new BigDecimal(buf.toString(StandardCharsets.US_ASCII));

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
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new BigDecimalValue((BigDecimal) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        short type = info.getType();
        return TypePredicates.isDecimal(type) || DataTypes.FLOAT == type || DataTypes.DOUBLE == type;
    }

    private static final class BigDecimalValue extends AbstractParameterValue {

        private final BigDecimal decimal;

        private BigDecimalValue(BigDecimal decimal) {
            this.decimal = decimal;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeStringifyNumber(decimal));
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
            if (!(o instanceof BigDecimalValue)) {
                return false;
            }

            BigDecimalValue that = (BigDecimalValue) o;

            return decimal.equals(that.decimal);
        }

        @Override
        public int hashCode() {
            return decimal.hashCode();
        }
    }
}
