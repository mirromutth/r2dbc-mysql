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

import io.github.mirromutth.r2dbc.mysql.constant.BinaryDateTimes;
import io.github.mirromutth.r2dbc.mysql.constant.DataTypes;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.github.mirromutth.r2dbc.mysql.internal.CodecUtils;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

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
    public LocalDate decode(NormalFieldValue value, FieldInformation info, Class<? super LocalDate> target, boolean binary, MySqlSession session) {
        ByteBuf buf = value.getBufferSlice();
        int index = buf.readerIndex();
        int bytes = buf.readableBytes();

        if (binary) {
            LocalDate date = readDateBinary(buf, bytes);

            if (date == null) {
                return ZeroDateHandler.handle(session.getZeroDateOption(), true, buf, index, bytes, ROUND);
            } else {
                return date;
            }
        } else {
            LocalDate date = readDateText(buf);

            if (date == null) {
                return ZeroDateHandler.handle(session.getZeroDateOption(), false, buf, index, bytes, ROUND);
            }

            return date;
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalDate;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new LocalDateValue((LocalDate) value);
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataTypes.DATE == info.getType();
    }

    @Nullable
    static LocalDate readDateText(ByteBuf buf) {
        int year = CodecUtils.readIntInDigits(buf, true);
        int month = CodecUtils.readIntInDigits(buf, true);
        int day = CodecUtils.readIntInDigits(buf, true);

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
