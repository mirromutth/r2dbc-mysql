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
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.chrono.ChronoLocalDateTime;

/**
 * Codec for {@link LocalDateTime} and {@link ChronoLocalDateTime}.
 * <p>
 * For now, supports A.D. calendar only in {@link ChronoLocalDateTime}.
 */
final class LocalDateTimeCodec implements ParametrizedCodec<LocalDateTime> {

    private static final LocalDateTime ROUND = LocalDateTime.of(LocalDateCodec.ROUND, LocalTime.MIN);

    private final ByteBufAllocator allocator;

    LocalDateTimeCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public LocalDateTime decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        return decode0(value, binary, context);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalDateTime;
    }

    @Override
    public ChronoLocalDateTime<LocalDate> decode(ByteBuf value, FieldInformation info, ParameterizedType target, boolean binary, CodecContext context) {
        return decode0(value, binary, context);
    }

    @Override
    public boolean canDecode(FieldInformation info, ParameterizedType target) {
        short type = info.getType();

        if (DataTypes.DATETIME != type && DataTypes.TIMESTAMP != type && DataTypes.TIMESTAMP2 != type) {
            return false;
        }

        Class<?> argument = ParametrizedUtils.getTypeArgument(target, ChronoLocalDateTime.class);

        if (argument == null) {
            return false;
        }

        return argument == LocalDate.class;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new LocalDateTimeParameter(allocator, (LocalDateTime) value);
    }

    @Override
    public boolean canDecode(FieldInformation info, Class<?> target) {
        short type = info.getType();
        return (DataTypes.DATETIME == type || DataTypes.TIMESTAMP == type || DataTypes.TIMESTAMP2 == type) &&
            target.isAssignableFrom(LocalDateTime.class);
    }

    @Nullable
    private static LocalDateTime decode0(ByteBuf value, boolean binary, CodecContext context) {
        int index = value.readerIndex();
        int bytes = value.readableBytes();
        LocalDateTime dateTime = binary ? decodeBinary(value, bytes) : decodeText(value);

        if (dateTime != null) {
            return dateTime;
        }

        return DateTimes.zeroDate(context.getZeroDateOption(), binary, value, index, bytes, ROUND);
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

        if (bytes < DateTimes.DATETIME_SIZE) {
            return LocalDateTime.of(date, LocalTime.MIDNIGHT);
        }

        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();

        if (bytes < DateTimes.MICRO_DATETIME_SIZE) {
            return LocalDateTime.of(date, LocalTime.of(hour, minute, second));
        }

        int nano = (int) (buf.readUnsignedIntLE() * DateTimes.NANOS_OF_MICRO);
        return LocalDateTime.of(date, LocalTime.of(hour, minute, second, nano));
    }

    private static final class LocalDateTimeParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final LocalDateTime value;

        private LocalDateTimeParameter(ByteBufAllocator allocator, LocalDateTime value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> {
                LocalTime time = value.toLocalTime();

                if (LocalTime.MIDNIGHT.equals(time)) {
                    return LocalDateCodec.encodeDate(allocator, value.toLocalDate());
                }

                int nano = time.getNano();
                int bytes = nano > 0 ? DateTimes.MICRO_DATETIME_SIZE : DateTimes.DATETIME_SIZE;
                ByteBuf buf = allocator.buffer(Byte.BYTES + bytes);

                try {
                    buf.writeByte(bytes)
                        .writeShortLE(value.getYear())
                        .writeByte(value.getMonthValue())
                        .writeByte(value.getDayOfMonth())
                        .writeByte(time.getHour())
                        .writeByte(time.getMinute())
                        .writeByte(time.getSecond());

                    if (nano > 0) {
                        return buf.writeIntLE(nano / DateTimes.NANOS_OF_MICRO);
                    }

                    return buf;
                } catch (Throwable e) {
                    buf.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> {
                LocalDateCodec.encodeDate(writer, value.toLocalDate());
                writer.append(' ');
                LocalTimeCodec.encodeTime(writer, value.toLocalTime());
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
            if (!(o instanceof LocalDateTimeParameter)) {
                return false;
            }

            LocalDateTimeParameter that = (LocalDateTimeParameter) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
