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
import reactor.core.publisher.Mono;

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
    public Year decode(NormalFieldValue value, FieldInformation info, Class<? super Year> target, boolean binary, MySqlSession session) {
        if (binary) {
            return Year.of(value.getBuffer().readShortLE());
        } else {
            return Year.of(IntegerCodec.parse(value.getBuffer()));
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Year;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return encodeOfYear((Year) value);
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataType.YEAR == info.getType();
    }

    private static ParameterValue encodeOfYear(Year year) {
        int value = year.getValue();

        if ((short) value != value) {
            // Unsupported, but should be considered here.
            return IntegerCodec.encodeOfInt(value);
        }

        return new YearValue((short) value);
    }

    private static final class YearValue extends AbstractParameterValue {

        private final short year;

        private YearValue(short year) {
            this.year = year;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeShort(year));
        }

        @Override
        public int getNativeType() {
            return DataType.YEAR.getType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof YearValue)) {
                return false;
            }

            YearValue yearValue = (YearValue) o;

            return year == yearValue.year;
        }

        @Override
        public int hashCode() {
            return (int) year;
        }
    }
}
