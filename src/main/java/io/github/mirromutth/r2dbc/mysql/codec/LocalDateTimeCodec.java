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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;

/**
 * Codec for {@link LocalDateTime}.
 */
final class LocalDateTimeCodec extends AbstractClassedCodec<LocalDateTime> {

    private static final LazyLoad<LocalDateTime> ROUND = LazyLoad.of(() -> LocalDateTime.of(LocalDateCodec.ROUND.get(), LocalTime.MIN));

    static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();

    private LocalDateTimeCodec() {
        super(LocalDateTime.class);
    }

    @Override
    public LocalDateTime decode(NormalFieldValue value, FieldInformation info, Class<? super LocalDateTime> target, boolean binary, MySqlSession session) {
        ByteBuf buf = value.getBuffer();
        int index = buf.readerIndex();
        int bytes = buf.readableBytes();

        if (binary) {
            TemporalAccessor accessor = JavaTimeHelper.readDateTimeBinary(buf);

            if (accessor == null) {
                return JavaTimeHelper.processZero(session.getZeroDateOption(), ROUND, () -> ByteBufUtil.hexDump(buf, index, bytes));
            } else if (accessor instanceof LocalDateTime) {
                return (LocalDateTime) accessor;
            } else if (accessor instanceof LocalDate) {
                return LocalDateTime.of((LocalDate) accessor, LocalTime.MIN);
            }

            // Must not null in here, do not use TemporalAccessor.query (it may return null)
            return LocalDateTime.from(accessor);
        } else {
            LocalDateTime dateTime = JavaTimeHelper.readDateTimeText(buf);

            if (dateTime == null) {
                return JavaTimeHelper.processZero(session.getZeroDateOption(), ROUND, () -> buf.toString(index, bytes, StandardCharsets.US_ASCII));
            }

            return dateTime;
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalDateTime;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new LocalDateTimeValue((LocalDateTime) value);
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        DataType type = info.getType();
        return (DataType.DATETIME == type || DataType.TIMESTAMP == type || DataType.TIMESTAMP2 == type);
    }

    private static final class LocalDateTimeValue extends AbstractParameterValue {

        private final LocalDateTime value;

        private LocalDateTimeValue(LocalDateTime value) {
            this.value = value;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeDateTime(value));
        }

        @Override
        public int getNativeType() {
            return DataType.DATETIME.getType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LocalDateTimeValue)) {
                return false;
            }

            LocalDateTimeValue that = (LocalDateTimeValue) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
