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
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import reactor.core.publisher.Mono;

import java.util.Arrays;

import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_BYTES;

/**
 * Codec for {@code byte[]}.
 */
final class ByteArrayCodec extends AbstractClassedCodec<byte[]> {

    ByteArrayCodec(ByteBufAllocator allocator) {
        super(allocator, byte[].class);
    }

    @Override
    public byte[] decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        if (!value.isReadable()) {
            return EMPTY_BYTES;
        }

        return ByteBufUtil.getBytes(value);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof byte[];
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new ByteArrayMySqlParameter(allocator, (byte[]) value);
    }

    @Override
    protected boolean doCanDecode(MySqlColumnMetadata metadata) {
        return metadata.getType().isBinary();
    }

    static ByteBuf encodeBytes(ByteBufAllocator alloc, byte[] value) {
        int size = value.length;

        if (size == 0) {
            // It is zero of var int, not terminal.
            return alloc.buffer(Byte.BYTES).writeByte(0);
        }

        ByteBuf buf = alloc.buffer(VarIntUtils.varIntBytes(size) + size);

        try {
            VarIntUtils.writeVarInt(buf, size);
            return buf.writeBytes(value);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    private static final class ByteArrayMySqlParameter extends AbstractMySqlParameter {

        private final ByteBufAllocator allocator;

        private final byte[] value;

        private ByteArrayMySqlParameter(ByteBufAllocator allocator, byte[] value) {
            this.allocator = allocator;
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> encodeBytes(allocator, value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeHex(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.VARBINARY;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ByteArrayMySqlParameter)) {
                return false;
            }

            ByteArrayMySqlParameter that = (ByteArrayMySqlParameter) o;

            return Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }
}
