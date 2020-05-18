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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

/**
 * Codec for {@link LocalDateTime}.
 */
final class LocalDateTimeCodec extends AbstractClassedCodec<LocalDateTime> {

    private static final LocalDateTime ROUND = LocalDateTime.of(LocalDateCodec.ROUND, LocalTime.MIN);

    static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();

    private LocalDateTimeCodec() {
        super(LocalDateTime.class);
    }

    @Override
    public LocalDateTime decode(ByteBuf value, FieldInformation info, Type target, boolean binary, ConnectionContext context) {
        int index = value.readerIndex();
        int bytes = value.readableBytes();

        if (binary) {
            LocalDateTime dateTime = decodeBinary(value, bytes);

            if (dateTime == null) {
                return CodecDateUtils.handle(context.getZeroDateOption(), true, value, index, bytes, ROUND);
            } else {
                return dateTime;
            }
        } else {
            LocalDateTime dateTime = decodeText(value);

            if (dateTime == null) {
                return CodecDateUtils.handle(context.getZeroDateOption(), false, value, index, bytes, ROUND);
            }

            return dateTime;
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalDateTime;
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new LocalDateTimeValue((LocalDateTime) value);
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        short type = info.getType();
        return DataTypes.DATETIME == type || DataTypes.TIMESTAMP == type || DataTypes.TIMESTAMP2 == type;
    }

    @Nullable
    private static LocalDateTime decodeText(ByteBuf buf) {
        LocalDate date = LocalDateCodec.readDateText(buf);

        if (date == null) {
            return null;
        }

        LocalTime time = LocalTimeCodec.readTimeText(buf);
        return LocalDateTime.of(date, time);
    }

    @Nullable
    private static LocalDateTime decodeBinary(ByteBuf buf, int bytes) {
        LocalDate date = LocalDateCodec.readDateBinary(buf, bytes);

        if (date == null) {
            return null;
        }

        if (bytes < BinaryDateTimes.DATETIME_SIZE) {
            return LocalDateTime.of(date, LocalTime.MIDNIGHT);
        }

        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();

        if (bytes < BinaryDateTimes.MICRO_DATETIME_SIZE) {
            return LocalDateTime.of(date, LocalTime.of(hour, minute, second));
        }

        long micros = buf.readUnsignedIntLE();

        return LocalDateTime.of(date, LocalTime.of(hour, minute, second, (int) TimeUnit.MICROSECONDS.toNanos(micros)));
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
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.fromRunnable(() -> {
                builder.append('\'');
                LocalDateCodec.encodeDate(builder, value.toLocalDate());
                builder.append(' ');
                LocalTimeCodec.encodeTime(builder, value.toLocalTime());
                builder.append('\'');
            });
        }

        @Override
        public short getType() {
            return DataTypes.DATETIME;
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
