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
    public BigDecimal decodeText(NormalFieldValue value, FieldInformation info, Class<? super BigDecimal> target, MySqlSession session) {
        return decodeBoth(value.getBuffer());
    }

    @Override
    public BigDecimal decodeBinary(NormalFieldValue value, FieldInformation info, Class<? super BigDecimal> target, MySqlSession session) {
        return decodeBoth(value.getBuffer());
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof BigDecimal;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new BigDecimalValue((BigDecimal) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return TypeConditions.isDecimal(info.getType());
    }

    private static BigDecimal decodeBoth(ByteBuf buf) {
        BigDecimal decimal = new BigDecimal(buf.toString(StandardCharsets.US_ASCII));

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
        public int getNativeType() {
            return DataType.NEW_DECIMAL.getType();
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
