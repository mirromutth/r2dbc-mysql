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
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_BYTES;

/**
 * Codec for {@link ByteBuffer}.
 */
final class ByteBufferCodec extends AbstractClassedCodec<ByteBuffer> {

    ByteBufferCodec(ByteBufAllocator allocator) {
        super(allocator, ByteBuffer.class);
    }

    @Override
    public ByteBuffer decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        if (!value.isReadable()) {
            return ByteBuffer.wrap(EMPTY_BYTES);
        }

        ByteBuffer result = ByteBuffer.allocate(value.readableBytes());

        value.readBytes(result);
        result.flip();

        return result;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new ByteBufferMySqlParameter(allocator, (ByteBuffer) value);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof ByteBuffer;
    }

    @Override
    protected boolean doCanDecode(MySqlColumnMetadata metadata) {
        return metadata.getType().isBinary();
    }

    private static final class ByteBufferMySqlParameter extends AbstractMySqlParameter {

        private final ByteBufAllocator allocator;

        private final ByteBuffer buffer;

        private ByteBufferMySqlParameter(ByteBufAllocator allocator, ByteBuffer buffer) {
            this.allocator = allocator;
            this.buffer = buffer;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> {
                if (!buffer.hasRemaining()) {
                    // It is zero of var int, not terminal.
                    return allocator.buffer(Byte.BYTES).writeByte(0);
                }

                int size = buffer.remaining();
                ByteBuf buf = allocator.buffer(VarIntUtils.varIntBytes(size) + size);

                try {
                    VarIntUtils.writeVarInt(buf, size);
                    return buf.writeBytes(buffer);
                } catch (Throwable e) {
                    buf.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeHex(buffer));
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
            if (!(o instanceof ByteBufferMySqlParameter)) {
                return false;
            }
            ByteBufferMySqlParameter that = (ByteBufferMySqlParameter) o;
            return buffer.equals(that.buffer);
        }

        @Override
        public int hashCode() {
            return buffer.hashCode();
        }
    }
}
