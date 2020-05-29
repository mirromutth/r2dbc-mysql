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

import dev.miku.r2dbc.mysql.message.FieldValue;
import dev.miku.r2dbc.mysql.message.LargeFieldValue;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.util.InternalArrays;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Codecs}.
 */
final class DefaultCodecs implements Codecs {

    private final Codec<?>[] codecs;

    private final ParametrizedCodec<?>[] parametrizedCodecs;

    private final MassiveCodec<?>[] massiveCodecs;

    private final MassiveParametrizedCodec<?>[] massiveParametrizedCodecs;

    private final Map<Type, PrimitiveCodec<?>> primitiveCodecs;

    private DefaultCodecs(Codec<?>[] codecs) {
        this.codecs = requireNonNull(codecs, "codecs must not be null");

        Map<Type, PrimitiveCodec<?>> primitiveCodecs = new HashMap<>();
        List<ParametrizedCodec<?>> parametrizedCodecs = new ArrayList<>();
        List<MassiveCodec<?>> massiveCodecs = new ArrayList<>();
        List<MassiveParametrizedCodec<?>> massiveParametrizedCodecs = new ArrayList<>();

        for (Codec<?> codec : codecs) {
            if (codec instanceof PrimitiveCodec<?>) {
                // Primitive codec must be class-based codec, cannot support ParameterizedType.
                PrimitiveCodec<?> c = (PrimitiveCodec<?>) codec;
                primitiveCodecs.put(c.getPrimitiveClass(), c);
            } else if (codec instanceof ParametrizedCodec<?>) {
                parametrizedCodecs.add((ParametrizedCodec<?>) codec);
            }

            if (codec instanceof MassiveCodec<?>) {
                massiveCodecs.add((MassiveCodec<?>) codec);

                if (codec instanceof MassiveParametrizedCodec<?>) {
                    massiveParametrizedCodecs.add((MassiveParametrizedCodec<?>) codec);
                }
            }
        }

        this.primitiveCodecs = primitiveCodecs;
        this.massiveCodecs = massiveCodecs.toArray(new MassiveCodec<?>[0]);
        this.massiveParametrizedCodecs = massiveParametrizedCodecs.toArray(new MassiveParametrizedCodec<?>[0]);
        this.parametrizedCodecs = parametrizedCodecs.toArray(new ParametrizedCodec<?>[0]);
    }

    /**
     * Note: this method should NEVER release {@code buf} because of
     * it come from {@code MySqlRow} which will release this buffer.
     */
    @Override
    public <T> T decode(FieldValue value, FieldInformation info, Class<?> type, boolean binary, CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(info, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        Class<?> target = chooseClass(info, type);

        // Fast map for primitive classes.
        if (target.isPrimitive()) {
            // If value is null field, then primitive codec should throw an exception rather than return null.
            return decodePrimitive(value, info, target, binary, context);
        } else if (value.isNull()) {
            // Not primitive classes and value is null field, return null.
            return null;
        } else if (value instanceof NormalFieldValue) {
            return decodeNormal((NormalFieldValue) value, info, target, binary, context);
        } else if (value instanceof LargeFieldValue) {
            return decodeMassive((LargeFieldValue) value, info, target, binary, context);
        }

        throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
    }

    @Override
    public <T> T decode(FieldValue value, FieldInformation info, ParameterizedType type, boolean binary, CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(info, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        if (value.isNull()) {
            return null;
        } else if (value instanceof NormalFieldValue) {
            return decodeNormal((NormalFieldValue) value, info, type, binary, context);
        } else if (value instanceof LargeFieldValue) {
            return decodeMassive((LargeFieldValue) value, info, type, binary, context);
        }

        throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
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
    public Parameter encode(Object value, CodecContext context) {
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
    public Parameter encodeNull() {
        return NullParameter.INSTANCE;
    }

    private <T> T decodePrimitive(FieldValue value, FieldInformation info, Class<?> type, boolean binary, CodecContext context) {
        if (value.isNull()) {
            throw new IllegalArgumentException(String.format("Cannot decode null for type %d", info.getType()));
        }

        @SuppressWarnings("unchecked")
        PrimitiveCodec<T> codec = (PrimitiveCodec<T>) this.primitiveCodecs.get(type);

        if (codec != null && value instanceof NormalFieldValue && codec.canPrimitiveDecode(info)) {
            return codec.decode(((NormalFieldValue) value).getBufferSlice(), info, type, binary, context);
        } else {
            // Mismatch, no one else can support this primitive class.
            throw new IllegalArgumentException(String.format("Cannot decode %s of %s for type %d", value.getClass().getSimpleName(), type, info.getType()));
        }
    }

    @Nullable
    private <T> T decodeNormal(NormalFieldValue value, FieldInformation info, Class<?> type, boolean binary, CodecContext context) {
        for (Codec<?> codec : codecs) {
            if (codec.canDecode(info, type)) {
                @SuppressWarnings("unchecked")
                Codec<T> c = (Codec<T>) codec;
                return c.decode(value.getBufferSlice(), info, type, binary, context);
            }
        }

        throw new IllegalArgumentException("Cannot decode value of type " + type + " for " + info.getType() + " with collation " + info.getCollationId());
    }

    @Nullable
    private <T> T decodeNormal(NormalFieldValue value, FieldInformation info, ParameterizedType type, boolean binary, CodecContext context) {
        for (ParametrizedCodec<?> codec : parametrizedCodecs) {
            if (codec.canDecode(info, type)) {
                @SuppressWarnings("unchecked")
                T result = (T) codec.decode(value.getBufferSlice(), info, type, binary, context);
                return result;
            }
        }

        throw new IllegalArgumentException("Cannot decode value of type " + type + " for " + info.getType() + " with collation " + info.getCollationId());
    }

    @Nullable
    private <T> T decodeMassive(LargeFieldValue value, FieldInformation info, Class<?> type, boolean binary, CodecContext context) {
        for (MassiveCodec<?> codec : massiveCodecs) {
            if (codec.canDecode(info, type)) {
                @SuppressWarnings("unchecked")
                MassiveCodec<T> c = (MassiveCodec<T>) codec;
                return c.decodeMassive(value.getBufferSlices(), info, type, binary, context);
            }
        }

        throw new IllegalArgumentException("Cannot decode massive value of type " + type + " for " + info.getType() + " with collation " + info.getCollationId());
    }

    @Nullable
    private <T> T decodeMassive(LargeFieldValue value, FieldInformation info, ParameterizedType type, boolean binary, CodecContext context) {
        for (MassiveParametrizedCodec<?> codec : massiveParametrizedCodecs) {
            if (codec.canDecode(info, type)) {
                @SuppressWarnings("unchecked")
                T result = (T) codec.decodeMassive(value.getBufferSlices(), info, type, binary, context);
                return result;
            }
        }

        throw new IllegalArgumentException("Cannot decode massive value of type " + type + " for " + info.getType() + " with collation " + info.getCollationId());
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

    private static Codec<?>[] defaultCodecs() {
        return new Codec<?>[]{
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
        };
    }

    static final class Holder implements Supplier<Codecs> {

        static final DefaultCodecs CODECS = new DefaultCodecs(defaultCodecs());

        private Holder() {
        }

        @Override
        public Codecs get() {
            return CODECS;
        }
    }

    static final class Builder extends ArrayList<Codec<?>> implements CodecsBuilder {

        @Override
        public CodecsBuilder addFirst(Codec<?> codec) {
            synchronized (this) {
                if (isEmpty()) {
                    addAll(InternalArrays.asImmutableList(defaultCodecs()));
                }
                add(0, codec);
            }
            return this;
        }

        @Override
        public CodecsBuilder addLast(Codec<?> codec) {
            synchronized (this) {
                if (isEmpty()) {
                    addAll(InternalArrays.asImmutableList(defaultCodecs()));
                }
                add(codec);
            }
            return this;
        }

        @Override
        public Codecs build() {
            synchronized (this) {
                if (isEmpty()) {
                    return Holder.CODECS;
                }
                return new DefaultCodecs(toArray(new Codec<?>[0]));
            }
        }
    }
}
