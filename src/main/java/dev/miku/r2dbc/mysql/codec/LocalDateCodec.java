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

import dev.miku.r2dbc.mysql.constant.BinaryDateTimes;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Type;
import java.time.LocalDate;

/**
 * Codec for {@link LocalDate}.
 */
final class LocalDateCodec extends AbstractClassedCodec<LocalDate> {

    static final LocalDate ROUND = LocalDate.of(1, 1, 1);

    static final LocalDateCodec INSTANCE = new LocalDateCodec();

    private LocalDateCodec() {
        super(LocalDate.class);
    }

    @Override
    public LocalDate decode(ByteBuf value, FieldInformation info, Type target, boolean binary, ConnectionContext context) {
        int index = value.readerIndex();
        int bytes = value.readableBytes();

        if (binary) {
            LocalDate date = readDateBinary(value, bytes);

            if (date == null) {
                return CodecDateUtils.handle(context.getZeroDateOption(), true, value, index, bytes, ROUND);
            } else {
                return date;
            }
        } else {
            LocalDate date = readDateText(value);

            if (date == null) {
                return CodecDateUtils.handle(context.getZeroDateOption(), false, value, index, bytes, ROUND);
            }

            return date;
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalDate;
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new LocalDateValue((LocalDate) value);
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataTypes.DATE == info.getType();
    }

    @Nullable
    static LocalDate readDateText(ByteBuf buf) {
        int year = CodecDateUtils.readIntInDigits(buf);
        int month = CodecDateUtils.readIntInDigits(buf);
        int day = CodecDateUtils.readIntInDigits(buf);

        if (month == 0 || day == 0) {
            return null;
        }

        return LocalDate.of(year, month, day);
    }

    @Nullable
    static LocalDate readDateBinary(ByteBuf buf, int bytes) {
        if (bytes < BinaryDateTimes.DATE_SIZE) {
            return null;
        }

        short year = buf.readShortLE();
        byte month = buf.readByte();
        byte day = buf.readByte();

        if (month == 0 || day == 0) {
            return null;
        }

        return LocalDate.of(year, month, day);
    }

    static void encodeDate(StringBuilder builder, LocalDate date) {
        boolean isNegative;
        int year = date.getYear();

        if (year < 0) {
            year = -year;
            isNegative = true;
        } else {
            isNegative = false;
        }

        if (isNegative) {
            builder.append('-');
        }

        // Note: year is the abs value of origin year.
        if (year < 1000) {
            builder.append('0');
            if (year < 100) {
                builder.append('0');
                if (year < 10) {
                    builder.append('0');
                }
            }
        }

        builder.append(year)
            .append('-');

        int month = date.getMonthValue();
        if (month < 10) {
            builder.append('0');
        }
        builder.append(month)
            .append('-');

        int day = date.getDayOfMonth();
        if (day < 10) {
            builder.append('0');
        }
        builder.append(day);
    }

    private static final class LocalDateValue extends AbstractParameterValue {

        private final LocalDate date;

        private LocalDateValue(LocalDate date) {
            this.date = date;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeDate(date));
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.fromRunnable(() -> {
                builder.append('\'');
                encodeDate(builder, date);
                builder.append('\'');
            });
        }

        @Override
        public short getType() {
            return DataTypes.DATE;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LocalDateValue)) {
                return false;
            }

            LocalDateValue that = (LocalDateValue) o;

            return date.equals(that.date);
        }

        @Override
        public int hashCode() {
            return date.hashCode();
        }
    }
}
