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
import io.netty.buffer.ByteBufUtil;
import reactor.core.publisher.Mono;

import java.util.BitSet;

import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_BYTES;

/**
 * Codec for {@link BitSet}.
 */
final class BitSetCodec extends AbstractClassedCodec<BitSet> {

    BitSetCodec(ByteBufAllocator allocator) {
        super(allocator, BitSet.class);
    }

    @Override
    public BitSet decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        if (!value.isReadable()) {
            return BitSet.valueOf(EMPTY_BYTES);
        }
        return BitSet.valueOf(reverse(ByteBufUtil.getBytes(value)));
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof BitSet;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new BitSetParameter(allocator, (BitSet) value);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return DataTypes.BIT == info.getType();
    }

    private static byte[] reverse(byte[] bytes) {
        int maxIndex = bytes.length - 1;
        int half = bytes.length >>> 1;
        byte b;

        for (int i = 0; i < half; ++i) {
            b = bytes[i];
            bytes[i] = bytes[maxIndex - i];
            bytes[maxIndex - i] = b;
        }

        return bytes;
    }

    private static final class BitSetParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final BitSet set;

        private BitSetParameter(ByteBufAllocator allocator, BitSet set) {
            this.allocator = allocator;
            this.set = set;
        }

        @Override
        public Mono<ByteBuf> binary() {
            return Mono.fromSupplier(() -> ByteArrayCodec.encodeBytes(allocator, reverse(set.toByteArray())));
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
            return Mono.fromRunnable(() -> {
                if (set.isEmpty()) {
                    // Must filled by 0 for MySQL 5.5.x, because MySQL 5.5.x does not clear its buffer on type BIT (i.e. unsafe allocate).
                    // So if we do not fill the buffer, it will use last content which is an undefined behavior. A classic bug, right?
                    writer.writeBinary(false);
                } else {
                    writer.writeHex(reverse(set.toByteArray()));
                }
            });
        }

        @Override
        public short getType() {
            return DataTypes.BIT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BitSetParameter)) {
                return false;
            }

            BitSetParameter that = (BitSetParameter) o;

            return set.equals(that.set);
        }

        @Override
        public int hashCode() {
            return set.hashCode();
        }
    }
}
