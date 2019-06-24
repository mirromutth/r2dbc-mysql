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

import io.github.mirromutth.r2dbc.mysql.constant.ColumnDefinitions;
import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.netty.buffer.ByteBuf;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Codec for {@link BigInteger}.
 */
final class BigIntegerCodec extends AbstractClassedCodec<BigInteger> {

    private static final String LONG_MAX_VALUE = Long.toString(Long.MAX_VALUE);

    static final BigIntegerCodec INSTANCE = new BigIntegerCodec();

    private BigIntegerCodec() {
        super(BigInteger.class);
    }

    @Override
    public BigInteger decodeText(NormalFieldValue value, FieldInformation info, Class<? super BigInteger> target, MySqlSession session) {
        ByteBuf buf = value.getBuffer();

        if (info.getType() == DataType.BIGINT && (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0) {
            if (buf.getByte(buf.readerIndex()) == '+') {
                buf.skipBytes(1);
            }

            String num = buf.toString(StandardCharsets.US_ASCII);

            if (isGreaterThanMaxValue(num)) {
                return new BigInteger(num);
            } else {
                // valueOf can use constant pool.
                return BigInteger.valueOf(parse(num));
            }
        } else {
            return BigInteger.valueOf(LongCodec.parse(buf));
        }
    }

    @Override
    public BigInteger decodeBinary(NormalFieldValue value, FieldInformation info, Class<? super BigInteger> target, MySqlSession session) {
        ByteBuf buf = value.getBuffer();
        boolean isUnsigned = (info.getDefinitions() & ColumnDefinitions.UNSIGNED) != 0;

        switch (info.getType()) {
            case BIGINT:
                long v = buf.readLongLE();
                if (isUnsigned) {
                    if (v >= 0) {
                        return BigInteger.valueOf(v);
                    } else {
                        return unsignedBigInteger(v);
                    }
                } else {
                    return BigInteger.valueOf(v);
                }
            case INT:
                if (isUnsigned) {
                    return BigInteger.valueOf(buf.readUnsignedIntLE());
                } else {
                    return BigInteger.valueOf(buf.readIntLE());
                }
            case MEDIUMINT:
                // Note: MySQL return 32-bits two's complement for 24-bits integer
                return BigInteger.valueOf(buf.readIntLE());
            case SMALLINT:
                if (isUnsigned) {
                    return BigInteger.valueOf(buf.readUnsignedShortLE());
                } else {
                    return BigInteger.valueOf(buf.readShortLE());
                }
            case YEAR:
                return BigInteger.valueOf(buf.readShortLE());
            default: // TINYINT
                if (isUnsigned) {
                    return BigInteger.valueOf(buf.readUnsignedByte());
                } else {
                    return BigInteger.valueOf(buf.readByte());
                }
        }
    }

    @Override
    public boolean canEncode(Object value) {
        // Do not check overflow because it should be ensured by user.
        return value instanceof BigInteger;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return LongCodec.encodeOfLong(((BigInteger) value).longValue());
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        return TypeConditions.isInt(info.getType());
    }

    private static boolean isGreaterThanMaxValue(String num) {
        int length = num.length();

        if (length != LONG_MAX_VALUE.length()) {
            // If length less than max value length, even it is 999...99, it is also less than max value.
            return length > LONG_MAX_VALUE.length();
        }

        return num.compareTo(LONG_MAX_VALUE) > 0;
    }

    private static long parse(String num) {
        long value = 0;
        int size = num.length();

        for (int i = 0; i < size; ++i) {
            value = value * 10L + (num.charAt(i) - '0');
        }

        return value;
    }

    static BigInteger unsignedBigInteger(long negative) {
        byte[] bits = new byte[Long.BYTES + 1];

        bits[0] = 0;
        bits[1] = (byte) (negative >>> 56);
        bits[2] = (byte) (negative >>> 48);
        bits[3] = (byte) (negative >>> 40);
        bits[4] = (byte) (negative >>> 32);
        bits[5] = (byte) (negative >>> 24);
        bits[6] = (byte) (negative >>> 16);
        bits[7] = (byte) (negative >>> 8);
        bits[8] = (byte) negative;

        return new BigInteger(bits);
    }
}
