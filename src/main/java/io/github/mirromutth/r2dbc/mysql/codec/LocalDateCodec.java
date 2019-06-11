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
import io.github.mirromutth.r2dbc.mysql.internal.LazyLoad;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;

/**
 * Codec for {@link LocalDate}.
 */
final class LocalDateCodec extends AbstractClassedCodec<LocalDate> {

    static final LazyLoad<LocalDate> ROUND = LazyLoad.of(() -> LocalDate.of(1, 1, 1));

    static final LocalDateCodec INSTANCE = new LocalDateCodec();

    private LocalDateCodec() {
        super(LocalDate.class);
    }

    @Override
    public LocalDate decodeText(NormalFieldValue value, FieldInformation info, Class<? super LocalDate> target, MySqlSession session) {
        ByteBuf buf = value.getBuffer();
        int index = buf.readerIndex();
        int bytes = buf.readableBytes();
        LocalDate date = JavaTimeHelper.readDateText(buf);

        if (date == null) {
            return JavaTimeHelper.processZero(session.getZeroDateOption(), ROUND, () -> buf.toString(index, bytes, StandardCharsets.US_ASCII));
        }

        return date;
    }

    @Override
    public LocalDate decodeBinary(NormalFieldValue value, FieldInformation info, Class<? super LocalDate> target, MySqlSession session) {
        ByteBuf buf = value.getBuffer();
        int index = buf.readerIndex();
        int bytes = buf.readableBytes();
        TemporalAccessor accessor = JavaTimeHelper.readDateTimeBinary(buf);

        if (accessor == null) {
            return JavaTimeHelper.processZero(session.getZeroDateOption(), ROUND, () -> ByteBufUtil.hexDump(buf, index, bytes));
        } else if (accessor instanceof LocalDate) {
            return (LocalDate) accessor;
        }

        // Must not null in here, do not use TemporalAccessor.query (it may return null)
        return LocalDate.from(accessor);
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
        return DataType.DATE == info.getType();
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
        public int getNativeType() {
            return DataType.DATE.getType();
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
