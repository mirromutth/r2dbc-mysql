/*
 * Copyright 2018-2021 the original author or authors.
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

import dev.miku.r2dbc.mysql.MySqlColumnMetadata;
import dev.miku.r2dbc.mysql.MySqlParameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.LocalDate;

/**
 * Codec for {@link LocalDate}.
 */
final class LocalDateCodec extends AbstractClassedCodec<LocalDate> {

    static final LocalDate ROUND = LocalDate.of(1, 1, 1);

    LocalDateCodec(ByteBufAllocator allocator) {
        super(allocator, LocalDate.class);
    }

    @Override
    public LocalDate decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        int bytes = value.readableBytes();
        LocalDate date = binary ? readDateBinary(value, bytes) : readDateText(value);

        if (date != null) {
            return date;
        }

        return DateTimes.zeroDate(context.getZeroDateOption(), binary, ROUND);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalDate;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new LocalDateMySqlParameter(allocator, (LocalDate) value);
    }

    @Override
    public boolean doCanDecode(MySqlColumnMetadata metadata) {
        return metadata.getType() == MySqlType.DATE;
    }

    @Nullable
    static LocalDate readDateText(ByteBuf buf) {
        int year = DateTimes.readIntInDigits(buf);
        int month = DateTimes.readIntInDigits(buf);
        int day = DateTimes.readIntInDigits(buf);

        if (month == 0 || day == 0) {
            return null;
        }

        return LocalDate.of(year, month, day);
    }

    @Nullable
    static LocalDate readDateBinary(ByteBuf buf, int bytes) {
        if (bytes < DateTimes.DATE_SIZE) {
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

    static ByteBuf encodeDate(ByteBufAllocator alloc, LocalDate date) {
        ByteBuf buf = alloc.buffer(Byte.BYTES + DateTimes.DATE_SIZE);

        try {
            return buf.writeByte(DateTimes.DATE_SIZE)
                .writeShortLE(date.getYear())
                .writeByte(date.getMonthValue())
                .writeByte(date.getDayOfMonth());
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    static void encodeDate(ParameterWriter writer, LocalDate date) {
        boolean isNegative;
        int year = date.getYear();

        if (year < 0) {
            year = -year;
            isNegative = true;
        } else {
            isNegative = false;
        }

        if (isNegative) {
            writer.append('-');
        } else {
            // Date start with number.
            writer.startString();
        }

        // Note: year is the abs value of origin year.
        if (year < 1000) {
            writer.append('0');
            if (year < 100) {
                writer.append('0');
                if (year < 10) {
                    writer.append('0');
                }
            }
        }

        writer.writeInt(year);
        writer.append('-');

        int month = date.getMonthValue();
        if (month < 10) {
            writer.append('0');
        }
        writer.writeInt(month);
        writer.append('-');

        int day = date.getDayOfMonth();
        if (day < 10) {
            writer.append('0');
        }
        writer.writeInt(day);
    }

    private static final class LocalDateMySqlParameter extends AbstractMySqlParameter {

        private final ByteBufAllocator allocator;

        private final LocalDate value;

        private LocalDateMySqlParameter(ByteBufAllocator allocator, LocalDate value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> encodeDate(allocator, value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> encodeDate(writer, value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.DATE;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LocalDateMySqlParameter)) {
                return false;
            }

            LocalDateMySqlParameter that = (LocalDateMySqlParameter) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
