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

import dev.miku.r2dbc.mysql.message.LargeFieldValue;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import dev.miku.r2dbc.mysql.message.FieldValue;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.message.ParameterValue;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Codecs}.
 */
final class DefaultCodecs implements Codecs {

    private final Codec<?>[] codecs;

    private final ParametrizedCodec<?>[] parametrizedCodecs;

    private final Map<Type, PrimitiveCodec<?>> primitiveCodecs;

    private DefaultCodecs(Codec<?>... codecs) {
        this.codecs = requireNonNull(codecs, "codecs must not be null");

        Map<Type, PrimitiveCodec<?>> primitiveCodecs = new HashMap<>();
        List<ParametrizedCodec<?>> parametrizedCodecs = new ArrayList<>();

        for (Codec<?> codec : codecs) {
            if (codec instanceof PrimitiveCodec<?>) {
                PrimitiveCodec<?> c = (PrimitiveCodec<?>) codec;
                primitiveCodecs.put(c.getPrimitiveClass(), c);
            } else if (codec instanceof ParametrizedCodec<?>) {
                parametrizedCodecs.add((ParametrizedCodec<?>) codec);
            }
        }

        this.primitiveCodecs = primitiveCodecs;
        this.parametrizedCodecs = parametrizedCodecs.toArray(new ParametrizedCodec<?>[0]);
    }

    /**
     * Note: this method should NEVER release {@code buf} because of
     * it come from {@code MySqlRow} which will release this buffer.
     */
    @Override
    public <T> T decode(FieldValue value, FieldInformation info, Class<?> type, boolean binary, ConnectionContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(info, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        Class<?> target = chooseClass(info, type);

        // Fast map for primitive classes.
        if (target.isPrimitive()) {
            if (value.isNull()) {
                throw new IllegalArgumentException(String.format("Cannot decode null for type %d", info.getType()));
            }

            @SuppressWarnings("unchecked")
            PrimitiveCodec<T> codec = (PrimitiveCodec<T>) this.primitiveCodecs.get(target);

            if (codec != null && value instanceof NormalFieldValue && codec.canPrimitiveDecode(info)) {
                return codec.decode(((NormalFieldValue) value).getBufferSlice(), info, target, binary, context);
            } else {
                // Mismatch, no one else can support this primitive class.
                throw new IllegalArgumentException(String.format("Cannot decode %s of %s for type %d", value.getClass().getSimpleName(), target, info.getType()));
            }
        }

        if (value.isNull()) {
            return null;
        }

        boolean massive = value instanceof LargeFieldValue;

        if (!massive && !(value instanceof NormalFieldValue)) {
            throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
        }

        for (Codec<?> codec : codecs) {
            if (codec.canDecode(massive, info, target)) {
                @SuppressWarnings("unchecked")
                Codec<T> c = (Codec<T>) codec;
                return massive ? c.decodeMassive(((LargeFieldValue) value).getBufferSlices(), info, target, binary, context)
                    : c.decode(((NormalFieldValue) value).getBufferSlice(), info, target, binary, context);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode %s of type %s for type %d with collation %d", value.getClass().getSimpleName(), target, info.getType(), info.getCollationId()));
    }

    @Override
    public <T> T decode(FieldValue value, FieldInformation info, ParameterizedType type, boolean binary, ConnectionContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(info, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        if (value.isNull()) {
            return null;
        }

        boolean massive = value instanceof LargeFieldValue;

        if (!massive && !(value instanceof NormalFieldValue)) {
            throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
        }

        for (ParametrizedCodec<?> codec : parametrizedCodecs) {
            if (codec.canDecode(massive, info, type)) {
                @SuppressWarnings("unchecked")
                T result = massive ? (T) codec.decodeMassive(((LargeFieldValue) value).getBufferSlices(), info, type, binary, context)
                    : (T) codec.decode(((NormalFieldValue) value).getBufferSlice(), info, type, binary, context);
                return result;
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode %s of type %s for type %d with collation %d", value.getClass().getSimpleName(), type, info.getType(), info.getCollationId()));

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

        for (Codec<?> codec : codecs) {
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

    static Codecs getDefault() {
        return new DefaultCodecs(
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
            LocalTimeCodec.INSTANCE,
            DurationCodec.INSTANCE,
            YearCodec.INSTANCE,

            StringCodec.INSTANCE,

            EnumCodec.INSTANCE,
            SetCodec.INSTANCE,

            ClobCodec.INSTANCE,
            BlobCodec.INSTANCE,

            ByteBufferCodec.INSTANCE,
            ByteArrayCodec.INSTANCE
        );
    }

    private static Class<?> chooseClass(FieldInformation info, Class<?> type) {
        // Object.class means could return any thing
        if (Object.class == type) {
            Class<?> mainType = info.getJavaType();

            if (mainType != null) {
                // use main type if main type exists
                return mainType;
            }
            // otherwise no main type, just use Object.class
        }

        return type;
    }
}
