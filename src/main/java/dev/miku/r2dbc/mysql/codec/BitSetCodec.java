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
import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.constant.MySqlType;
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
    public BitSet decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        if (!value.isReadable()) {
            return BitSet.valueOf(EMPTY_BYTES);
        }

        // Result with big-endian, BitSet is using little-endian, need reverse.
        return BitSet.valueOf(reverse(ByteBufUtil.getBytes(value)));
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof BitSet;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        BitSet set = (BitSet) value;
        long bits;
        if (set.isEmpty()) {
            bits = 0;
        } else {
            long[] array = set.toLongArray();

            // The max precision of BIT is 64, so just use the first Long.
            if (array.length == 0) {
                bits = 0;
            } else {
                bits = array[0];
            }
        }

        MySqlType type;

        if ((byte) bits == bits) {
            type = MySqlType.TINYINT;
        } else if ((short) bits == bits) {
            type = MySqlType.SMALLINT;
        } else if ((int) bits == bits) {
            type = MySqlType.INT;
        } else {
            type = MySqlType.BIGINT;
        }

        return new BitSetParameter(allocator, bits, type);
    }

    @Override
    protected boolean doCanDecode(MySqlColumnMetadata metadata) {
        return metadata.getType() == MySqlType.BIT;
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

        private final long value;

        private final MySqlType type;

        private BitSetParameter(ByteBufAllocator allocator, long value, MySqlType type) {
            this.allocator = allocator;
            this.value = value;
            this.type = type;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            switch (type) {
                case TINYINT:
                    return Mono.fromSupplier(() -> allocator.buffer(Byte.BYTES).writeByte((int) value));
                case SMALLINT:
                    return Mono.fromSupplier(() -> allocator.buffer(Short.BYTES).writeShortLE((int) value));
                case INT:
                    return Mono.fromSupplier(() -> allocator.buffer(Integer.BYTES).writeIntLE((int) value));
                default: // BIGINT
                    return Mono.fromSupplier(() -> allocator.buffer(Long.BYTES).writeLongLE(value));
            }
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> {
                if (value == 0) {
                    // Must filled by 0 for MySQL 5.5.x, because MySQL 5.5.x does not clear its buffer on type
                    // BIT (i.e. unsafe allocate).
                    // So if we do not fill the buffer, it will use last content which is an undefined
                    // behavior. A classic bug, right?
                    writer.writeBinary(false);
                } else {
                    writer.writeHex(value);
                }
            });
        }

        @Override
        public MySqlType getType() {
            return type;
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

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }
}
