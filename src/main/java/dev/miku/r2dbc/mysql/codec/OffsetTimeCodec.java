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

import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Codec for {@link OffsetTime}.
 */
final class OffsetTimeCodec extends AbstractClassedCodec<OffsetTime> {

    OffsetTimeCodec(ByteBufAllocator allocator) {
        super(allocator, OffsetTime.class);
    }

    @Override
    public OffsetTime decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary,
        CodecContext context) {
        LocalTime origin = LocalTimeCodec.decodeOrigin(binary, value);
        ZoneId zone = context.getServerZoneId();

        return OffsetTime.of(origin, zone instanceof ZoneOffset ? (ZoneOffset) zone : zone.getRules()
            .getStandardOffset(Instant.EPOCH));
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new OffsetTimeParameter(allocator, (OffsetTime) value, context);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof OffsetTime;
    }

    @Override
    public boolean doCanDecode(FieldInformation info) {
        return DataTypes.TIME == info.getType();
    }

    private static final class OffsetTimeParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final OffsetTime value;

        private final CodecContext context;

        private OffsetTimeParameter(ByteBufAllocator allocator, OffsetTime value, CodecContext context) {
            this.allocator = allocator;
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> LocalTimeCodec.encodeBinary(allocator, serverValue()));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> LocalTimeCodec.encodeTime(writer, serverValue()));
        }

        @Override
        public short getType() {
            return DataTypes.TIME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof OffsetTimeParameter)) {
                return false;
            }

            OffsetTimeParameter that = (OffsetTimeParameter) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        private LocalTime serverValue() {
            ZoneId zone = context.getServerZoneId();
            ZoneOffset offset = zone instanceof ZoneOffset ? (ZoneOffset) zone : zone.getRules()
                .getStandardOffset(Instant.EPOCH);

            return value.toLocalTime()
                .plusSeconds(offset.getTotalSeconds() - value.getOffset().getTotalSeconds());
        }
    }
}
