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

import java.time.Instant;
import java.time.LocalDateTime;

/**
 * Codec for {@link Instant}.
 */
final class InstantCodec implements Codec<Instant> {

    private final ByteBufAllocator allocator;

    InstantCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public Instant decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        LocalDateTime origin = LocalDateTimeCodec.decodeOrigin(value, binary, context);

        if (origin == null) {
            return null;
        }

        return origin.toInstant(context.getServerZoneId().getRules().getOffset(origin));
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new InstantMySqlParameter(allocator, (Instant) value, context);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Instant;
    }

    @Override
    public boolean canDecode(MySqlColumnMetadata metadata, Class<?> target) {
        return DateTimes.canDecodeDateTime(metadata.getType(), target, Instant.class);
    }

    private static final class InstantMySqlParameter extends AbstractMySqlParameter {

        private final ByteBufAllocator allocator;

        private final Instant value;

        private final CodecContext context;

        private InstantMySqlParameter(ByteBufAllocator allocator, Instant value, CodecContext context) {
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
        public MySqlType getType() {
            return MySqlType.TIMESTAMP;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InstantMySqlParameter that = (InstantMySqlParameter) o;
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        private LocalDateTime serverValue() {
            return LocalDateTime.ofInstant(value, context.getServerZoneId());
        }
    }
}
