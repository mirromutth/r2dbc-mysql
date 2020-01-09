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
import dev.miku.r2dbc.mysql.util.CodecUtils;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import reactor.core.publisher.Mono;

import java.util.Arrays;

import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_BYTES;

/**
 * Codec for {@link byte[]}.
 */
final class ByteArrayCodec extends AbstractClassedCodec<byte[]> {

    static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

    private ByteArrayCodec() {
        super(byte[].class);
    }

    @Override
    public byte[] decode(NormalFieldValue value, FieldInformation info, Class<? super byte[]> target, boolean binary, ConnectionContext context) {
        ByteBuf buf = value.getBufferSlice();

        if (!buf.isReadable()) {
            return EMPTY_BYTES;
        }
        return ByteBufUtil.getBytes(buf);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof byte[];
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new ByteArrayValue((byte[]) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return TypePredicates.isBinary(info.getType());
    }

    private static final class ByteArrayValue extends AbstractParameterValue {

        private final byte[] bytes;

        private ByteArrayValue(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeByteArray(bytes));
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.fromRunnable(() -> {
                builder.append('x').append('\'');
                CodecUtils.appendHex(builder, bytes, false);
                builder.append('\'');
            });
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
            if (!(o instanceof ByteArrayValue)) {
                return false;
            }

            ByteArrayValue that = (ByteArrayValue) o;

            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}
