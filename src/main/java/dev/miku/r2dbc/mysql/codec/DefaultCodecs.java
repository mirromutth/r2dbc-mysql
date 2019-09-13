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

import dev.miku.r2dbc.mysql.internal.ConnectionContext;
import dev.miku.r2dbc.mysql.message.FieldValue;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.message.ParameterValue;

import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static dev.miku.r2dbc.mysql.internal.AssertUtils.requireNonNull;

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
    public <T> T decode(FieldValue value, FieldInformation info, Type type, boolean binary, ConnectionContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(info, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        Type target = chooseTarget(info, type);

        // Fast map for primitive classes.
        if (target instanceof Class<?> && ((Class<?>) target).isPrimitive()) {
            if (value.isNull()) {
                throw new IllegalArgumentException(String.format("Cannot decode null for type %d", info.getType()));
            }

            @SuppressWarnings("unchecked")
            Class<T> targetClass = (Class<T>) target;
            @SuppressWarnings("unchecked")
            PrimitiveCodec<T> codec = (PrimitiveCodec<T>) this.primitiveCodecs.get(targetClass);

            if (codec != null && value instanceof NormalFieldValue && codec.canPrimitiveDecode(info)) {
                return codec.decode((NormalFieldValue) value, info, targetClass, binary, context);
            } else {
                // Primitive mismatch, no `Codec` support this primitive class.
                throw new IllegalArgumentException(String.format("Cannot decode value of type %s for type %d", targetClass, info.getType()));
            }
        }

        if (value.isNull()) {
            return null;
        }

        for (Codec<?, ?, ?> codec : codecs) {
            if (codec.canDecode(value, info, target)) {
                @SuppressWarnings("unchecked")
                Codec<T, ? super FieldValue, ? super Type> c = (Codec<T, ? super FieldValue, ? super Type>) codec;
                return c.decode(value, info, target, binary, context);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode value of type %s for type %d", target, info.getType()));
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
    public ParameterValue encode(Object value, ConnectionContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(context, "context must not be null");

        for (Codec<?, ?, ?> codec : codecs) {
            if (codec.canEncode(value)) {
                return codec.encode(value, context);
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
