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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_BYTES;

/**
 * Codec for {@link ByteBuffer}.
 */
final class ByteBufferCodec extends AbstractClassedCodec<ByteBuffer> {

    static final ByteBufferCodec INSTANCE = new ByteBufferCodec();

    private ByteBufferCodec() {
        super(ByteBuffer.class);
    }

    @Override
    public ByteBuffer decode(NormalFieldValue value, FieldInformation info, Class<? super ByteBuffer> target, boolean binary, ConnectionContext context) {
        ByteBuf buf = value.getBufferSlice();

        if (!buf.isReadable()) {
            return ByteBuffer.wrap(EMPTY_BYTES);
        }

        ByteBuffer result = ByteBuffer.allocate(buf.readableBytes());

        buf.readBytes(result);
        result.flip();

        return result;
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new ByteBufferValue((ByteBuffer) value);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof ByteBuffer;
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return TypePredicates.isBinary(info.getType());
    }

    private static final class ByteBufferValue extends AbstractParameterValue {

        private final ByteBuffer buffer;

        private ByteBufferValue(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeByteBuffer(buffer));
        }

        @Override
        public short getType() {
            return DataTypes.LONG_BLOB;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ByteBufferValue)) {
                return false;
            }
            ByteBufferValue that = (ByteBufferValue) o;
            return buffer.equals(that.buffer);
        }

        @Override
        public int hashCode() {
            return buffer.hashCode();
        }
    }
}
