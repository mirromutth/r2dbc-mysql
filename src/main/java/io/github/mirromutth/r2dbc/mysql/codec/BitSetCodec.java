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

package io.github.mirromutth.r2dbc.mysql.codec;

import io.github.mirromutth.r2dbc.mysql.constant.DataTypes;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import reactor.core.publisher.Mono;

import java.util.BitSet;

import static io.github.mirromutth.r2dbc.mysql.constant.EmptyArrays.EMPTY_BYTES;

/**
 * Codec for {@link BitSet}.
 */
final class BitSetCodec extends AbstractClassedCodec<BitSet> {

    static final BitSetCodec INSTANCE = new BitSetCodec();

    private BitSetCodec() {
        super(BitSet.class);
    }

    @Override
    public BitSet decode(NormalFieldValue value, FieldInformation info, Class<? super BitSet> target, boolean binary, MySqlSession session) {
        ByteBuf buf = value.getBufferSlice();

        if (!buf.isReadable()) {
            return BitSet.valueOf(EMPTY_BYTES);
        }
        return BitSet.valueOf(reverse(ByteBufUtil.getBytes(buf)));
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof BitSet;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new BitSetValue((BitSet) value);
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

    private static final class BitSetValue extends AbstractParameterValue {

        private final BitSet set;

        private BitSetValue(BitSet set) {
            this.set = set;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeByteArray(reverse(set.toByteArray())));
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
            if (!(o instanceof BitSetValue)) {
                return false;
            }

            BitSetValue that = (BitSetValue) o;

            return set.equals(that.set);
        }

        @Override
        public int hashCode() {
            return set.hashCode();
        }
    }
}
