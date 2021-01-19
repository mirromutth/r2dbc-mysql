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
import java.time.ZonedDateTime;
import java.time.chrono.ChronoZonedDateTime;

/**
 * Codec for {@link ZonedDateTime} and {@link ChronoZonedDateTime}.
 * <p>
 * For now, supports only A.D. calendar in {@link ChronoZonedDateTime}.
 */
final class ZonedDateTimeCodec implements ParametrizedCodec<ZonedDateTime> {

    private final ByteBufAllocator allocator;

    ZonedDateTimeCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public ZonedDateTime decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary,
        CodecContext context) {
        return decode0(value, binary, context);
    }

    @Override
    public ChronoZonedDateTime<LocalDate> decode(ByteBuf value, FieldInformation info,
        ParameterizedType target, boolean binary, CodecContext context) {
        return decode0(value, binary, context);
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new ZonedDateTimeParameter(allocator, (ZonedDateTime) value, context);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof ZonedDateTime;
    }

    @Override
    public boolean canDecode(FieldInformation info, ParameterizedType target) {
        return DateTimes.canDecodeChronology(info.getType(), target, ChronoZonedDateTime.class);
    }

    @Override
    public boolean canDecode(FieldInformation info, Class<?> target) {
        return DateTimes.canDecodeDateTime(info.getType(), target, ZonedDateTime.class);
    }

    @Nullable
    private static ZonedDateTime decode0(ByteBuf value, boolean binary, CodecContext context) {
        LocalDateTime origin = LocalDateTimeCodec.decodeOrigin(value, binary, context);
        return origin == null ? null : ZonedDateTime.of(origin, context.getServerZoneId());
    }

    private static final class ZonedDateTimeParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final ZonedDateTime value;

        private final CodecContext context;

        private ZonedDateTimeParameter(ByteBufAllocator allocator, ZonedDateTime value,
            CodecContext context) {
            this.allocator = allocator;
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> LocalDateTimeCodec.encodeBinary(allocator, serverValue()));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> LocalDateTimeCodec.encodeText(writer, serverValue()));
        }

        @Override
        public short getType() {
            return DataTypes.TIMESTAMP;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ZonedDateTimeParameter)) {
                return false;
            }

            ZonedDateTimeParameter that = (ZonedDateTimeParameter) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        private LocalDateTime serverValue() {
            return value.withZoneSameInstant(context.getServerZoneId())
                .toLocalDateTime();
        }
    }
}
