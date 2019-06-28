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

import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.FieldValue;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Codecs}.
 */
final class DefaultCodecs implements Codecs {

    static final DefaultCodecs INSTANCE = new DefaultCodecs(
        ByteCodec.INSTANCE,
        ShortCodec.INSTANCE,
        IntegerCodec.INSTANCE,
        LongCodec.INSTANCE,
        BigIntegerCodec.INSTANCE,

        BigDecimalCodec.INSTANCE, // Only all decimals
        FloatCodec.INSTANCE, // Decimal (precision < 7) or float
        DoubleCodec.INSTANCE, // Decimal (precision < 16) or double or float

        BooleanCodec.INSTANCE,
        BitSetCodec.INSTANCE,

        LocalDateTimeCodec.INSTANCE,
        LocalDateCodec.INSTANCE,
        DurationCodec.INSTANCE,
        LocalTimeCodec.INSTANCE,
        YearCodec.INSTANCE,

        StringCodec.INSTANCE,

        EnumCodec.INSTANCE,
        SetCodec.INSTANCE,
        StringArrayCodec.INSTANCE,

        ClobCodec.INSTANCE,
        BlobCodec.INSTANCE,

        ByteArrayCodec.INSTANCE
    );

    private final Codec<?, ?, ?>[] codecs;

    private final Map<Class<?>, PrimitiveCodec<?>> primitiveCodecs;

    private DefaultCodecs(Codec<?, ?, ?>... codecs) {
        this.codecs = requireNonNull(codecs, "codecs must not be null");

        Map<Class<?>, PrimitiveCodec<?>> primitiveCodecs = new HashMap<>();

        for (Codec<?, ?, ?> codec : codecs) {
            if (codec instanceof PrimitiveCodec<?>) {
                PrimitiveCodec<?> c = (PrimitiveCodec<?>) codec;
                primitiveCodecs.put(c.getPrimitiveClass(), c);
            }
        }

        this.primitiveCodecs = primitiveCodecs;
    }

    /**
     * Note: this method should NEVER release {@code buf} because of
     * it come from {@code MySqlRow} which will release this buffer.
     */
    @Override
    public <T> T decode(boolean binary, FieldValue value, FieldInformation info, Type type, MySqlSession session) {
        requireNonNull(value, "value must not be null");
        requireNonNull(info, "info must not be null");
        requireNonNull(session, "session must not be null");
        requireNonNull(type, "type must not be null");

        if (value.isNull()) {
            return null;
        }

        if (DataType.UNKNOWN == info.getType()) {
            throw new IllegalArgumentException(String.format("Unknown native column type %d, can not decode", info.getNativeTypeMetadata()));
        }

        Type target = chooseTarget(info, type);

        // Fast map for primitive classes.
        if (target instanceof Class<?> && ((Class<?>) target).isPrimitive()) {
            @SuppressWarnings("unchecked")
            Class<T> targetClass = (Class<T>) target;
            @SuppressWarnings("unchecked")
            PrimitiveCodec<T> codec = (PrimitiveCodec<T>) this.primitiveCodecs.get(targetClass);

            if (codec != null && value instanceof NormalFieldValue && codec.canPrimitiveDecode(info)) {
                if (binary) {
                    return codec.decodeBinary((NormalFieldValue) value, info, targetClass, session);
                } else {
                    return codec.decodeText((NormalFieldValue) value, info, targetClass, session);
                }
            } else {
                // Primitive mismatch, no `Codec` support this primitive class.
                throw new IllegalArgumentException(String.format("Cannot decode value of type '%s' with column type '%s'", targetClass, info.getType()));
            }
        }

        for (Codec<?, ?, ?> codec : codecs) {
            if (codec.canDecode(value, info, target)) {
                @SuppressWarnings("unchecked")
                Codec<T, ? super FieldValue, ? super Type> c = (Codec<T, ? super FieldValue, ? super Type>) codec;
                if (binary) {
                    return c.decodeBinary(value, info, target, session);
                } else {
                    return c.decodeText(value, info, target, session);
                }
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode value of type '%s' with column type '%s'", target, info.getType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decodeLastInsertId(long value, Class<T> type) {
        requireNonNull(type, "type must not be null");

        if (Byte.TYPE == type || Byte.class == type) {
            return (T) Byte.valueOf((byte) value);
        } else if (Short.TYPE == type || Short.class == type) {
            return (T) Short.valueOf((short) value);
        } else if (Integer.TYPE == type || Integer.class == type) {
            return (T) Integer.valueOf((int) value);
        } else if (Long.TYPE == type || Long.class == type) {
            return (T) Long.valueOf(value);
        } else if (BigInteger.class == type) {
            if (value < 0) {
                 return (T) BigIntegerCodec.unsignedBigInteger(value);
            } else {
                return (T) BigInteger.valueOf(value);
            }
        } else if (type.isAssignableFrom(Number.class)) {
            if (value < 0) {
                return (T) BigIntegerCodec.unsignedBigInteger(value);
            } else {
                return (T) Long.valueOf(value);
            }
        }

        String message = "Cannot decode value of type '%s' with last inserted ID %s";
        if (value < 0) {
            message = String.format(message, type, Long.toUnsignedString(value));
        } else {
            message = String.format(message, type, value);
        }
        throw new IllegalArgumentException(message);
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        requireNonNull(value, "value must not be null");
        requireNonNull(session, "session must not be null");

        for (Codec<?, ?, ?> codec : codecs) {
            if (codec.canEncode(value)) {
                return codec.encode(value, session);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot encode value of type '%s'", value.getClass()));
    }

    @Override
    public ParameterValue encodeNull() {
        return NullParameterValue.INSTANCE;
    }

    private static Type chooseTarget(FieldInformation info, Type target) {
        // Object.class means could return any thing
        if (Object.class == target) {
            Class<?> mainType = info.getJavaType();

            if (mainType != null) {
                // use main type if main type exists
                return mainType;
            }
            // otherwise no main type, just use Object.class
        }

        return target;
    }
}
