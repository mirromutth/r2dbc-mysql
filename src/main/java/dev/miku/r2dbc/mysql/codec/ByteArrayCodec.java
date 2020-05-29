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

import dev.miku.r2dbc.mysql.ParameterOutputStream;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.Parameter;
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
    public byte[] decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
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
    public Parameter encode(Object value, CodecContext context) {
        return new ByteArrayParameter((byte[]) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return TypePredicates.isBinary(info.getType());
    }

    private static final class ByteArrayParameter extends AbstractParameter {

        private final byte[] bytes;

        private ByteArrayParameter(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public Mono<Void> binary(ParameterOutputStream output) {
            return Mono.fromRunnable(() -> output.writeByteArray(bytes));
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeHex(bytes));
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
            if (!(o instanceof ByteArrayParameter)) {
                return false;
            }

            ByteArrayParameter that = (ByteArrayParameter) o;

            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}
