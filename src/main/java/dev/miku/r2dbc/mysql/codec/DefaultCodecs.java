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
import dev.miku.r2dbc.mysql.message.FieldValue;
import dev.miku.r2dbc.mysql.message.LargeFieldValue;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.util.InternalArrays;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

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

    private final MassiveCodec<?>[] massiveCodecs;

    private final MassiveParametrizedCodec<?>[] massiveParametrizedCodecs;

    private final Map<Type, PrimitiveCodec<?>> primitiveCodecs;

    private DefaultCodecs(Codec<?>[] codecs) {
        this.codecs = requireNonNull(codecs, "codecs must not be null");

        Map<Type, PrimitiveCodec<?>> primitiveCodecs = new HashMap<>();
        List<ParametrizedCodec<?>> parametrizedCodecs = new ArrayList<>();
        List<MassiveCodec<?>> massiveCodecs = new ArrayList<>();
        List<MassiveParametrizedCodec<?>> massiveParamCodecs = new ArrayList<>();

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
                    massiveParamCodecs.add((MassiveParametrizedCodec<?>) codec);
                }
            }
        }

        this.primitiveCodecs = primitiveCodecs;
        this.massiveCodecs = massiveCodecs.toArray(new MassiveCodec<?>[0]);
        this.massiveParametrizedCodecs = massiveParamCodecs.toArray(new MassiveParametrizedCodec<?>[0]);
        this.parametrizedCodecs = parametrizedCodecs.toArray(new ParametrizedCodec<?>[0]);
    }

    /**
     * Note: this method should NEVER release {@code buf} because of it come from {@code MySqlRow} which will
     * release this buffer.
     */
    @Override
    public <T> T decode(FieldValue value, MySqlColumnMetadata metadata, Class<?> type, boolean binary,
        CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(metadata, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        if (value.isNull()) {
            // T is always an object, so null should be returned even if the type is a primitive class.
            // See also https://github.com/mirromutth/r2dbc-mysql/issues/184 .
            return null;
        }

        Class<?> target = chooseClass(metadata, type);

        // Fast map for primitive classes.
        if (target.isPrimitive()) {
            return decodePrimitive(value, metadata, target, binary, context);
        } else if (value instanceof NormalFieldValue) {
            return decodeNormal((NormalFieldValue) value, metadata, target, binary, context);
        } else if (value instanceof LargeFieldValue) {
            return decodeMassive((LargeFieldValue) value, metadata, target, binary, context);
        }

        throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
    }

    @Override
    public <T> T decode(FieldValue value, MySqlColumnMetadata metadata, ParameterizedType type,
        boolean binary, CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(metadata, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        if (value.isNull()) {
            return null;
        } else if (value instanceof NormalFieldValue) {
            return decodeNormal((NormalFieldValue) value, metadata, type, binary, context);
        } else if (value instanceof LargeFieldValue) {
            return decodeMassive((LargeFieldValue) value, metadata, type, binary, context);
        }

        throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decodeLastInsertId(long value, Class<?> type) {
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
                return (T) CodecUtils.unsignedBigInteger(value);
            }

            return (T) BigInteger.valueOf(value);
        } else if (type.isAssignableFrom(Number.class)) {
            if (value < 0) {
                return (T) CodecUtils.unsignedBigInteger(value);
            }

            return (T) Long.valueOf(value);
        }

        throw new IllegalArgumentException(String.format("Cannot decode %s with last inserted ID %s", type,
            value < 0 ? Long.toUnsignedString(value) : value));
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

        throw new IllegalArgumentException("Cannot encode " + value.getClass());
    }

    @Override
    public Parameter encodeNull() {
        return NullParameter.INSTANCE;
    }

    private <T> T decodePrimitive(FieldValue value, MySqlColumnMetadata metadata, Class<?> type,
        boolean binary, CodecContext context) {
        @SuppressWarnings("unchecked")
        PrimitiveCodec<T> codec = (PrimitiveCodec<T>) this.primitiveCodecs.get(type);

        if (codec != null && value instanceof NormalFieldValue && codec.canPrimitiveDecode(metadata)) {
            return codec.decode(((NormalFieldValue) value).getBufferSlice(), metadata, type, binary, context);
        }

        // Mismatch, no one else can support this primitive class.
        throw new IllegalArgumentException("Cannot decode " + value.getClass().getSimpleName() + " of " +
            type + " for " + metadata.getType());
    }

    @Nullable
    private <T> T decodeNormal(NormalFieldValue value, MySqlColumnMetadata metadata, Class<?> type,
        boolean binary, CodecContext context) {
        for (Codec<?> codec : codecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                Codec<T> c = (Codec<T>) codec;
                return c.decode(value.getBufferSlice(), metadata, type, binary, context);
            }
        }

        throw new IllegalArgumentException("Cannot decode " + type + " for " + metadata.getType());
    }

    @Nullable
    private <T> T decodeNormal(NormalFieldValue value, MySqlColumnMetadata metadata, ParameterizedType type,
        boolean binary, CodecContext context) {
        for (ParametrizedCodec<?> codec : parametrizedCodecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                T result = (T) codec.decode(value.getBufferSlice(), metadata, type, binary, context);
                return result;
            }
        }

        throw new IllegalArgumentException("Cannot decode " + type + " for " + metadata.getType());
    }

    @Nullable
    private <T> T decodeMassive(LargeFieldValue value, MySqlColumnMetadata metadata, Class<?> type,
        boolean binary, CodecContext context) {
        for (MassiveCodec<?> codec : massiveCodecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                MassiveCodec<T> c = (MassiveCodec<T>) codec;
                return c.decodeMassive(value.getBufferSlices(), metadata, type, binary, context);
            }
        }

        throw new IllegalArgumentException("Cannot decode massive " + type + " for " + metadata.getType());
    }

    @Nullable
    private <T> T decodeMassive(LargeFieldValue value, MySqlColumnMetadata metadata, ParameterizedType type,
        boolean binary, CodecContext context) {
        for (MassiveParametrizedCodec<?> codec : massiveParametrizedCodecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                T result = (T) codec.decodeMassive(value.getBufferSlices(), metadata, type, binary, context);
                return result;
            }
        }

        throw new IllegalArgumentException("Cannot decode massive  " + type + " for " + metadata.getType());
    }

    private static Class<?> chooseClass(MySqlColumnMetadata metadata, Class<?> type) {
        Class<?> javaType = metadata.getType().getJavaType();;
        return type.isAssignableFrom(javaType) ? javaType : type;
    }

    private static Codec<?>[] defaultCodecs(ByteBufAllocator allocator) {
        return new Codec<?>[] {
            new ByteCodec(allocator),
            new ShortCodec(allocator),
            new IntegerCodec(allocator),
            new LongCodec(allocator),
            new BigIntegerCodec(allocator),

            new BigDecimalCodec(allocator), // Only all decimals
            new FloatCodec(allocator), // Decimal (precision < 7) or float
            new DoubleCodec(allocator), // Decimal (precision < 16) or double or float

            new BooleanCodec(allocator),
            new BitSetCodec(allocator),

            new ZonedDateTimeCodec(allocator),
            new LocalDateTimeCodec(allocator),
            new InstantCodec(allocator),
            new OffsetDateTimeCodec(allocator),

            new LocalDateCodec(allocator),

            new LocalTimeCodec(allocator),
            new DurationCodec(allocator),
            new OffsetTimeCodec(allocator),

            new YearCodec(allocator),

            new StringCodec(allocator),

            new EnumCodec(allocator),
            new SetCodec(allocator),

            new ClobCodec(allocator),
            new BlobCodec(allocator),

            new ByteBufferCodec(allocator),
            new ByteArrayCodec(allocator)
        };
    }

    static final class Builder extends ArrayList<Codec<?>> implements CodecsBuilder {

        private final ByteBufAllocator allocator;

        Builder(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public CodecsBuilder addFirst(Codec<?> codec) {
            synchronized (this) {
                if (isEmpty()) {
                    Codec<?>[] codecs = defaultCodecs(allocator);

                    ensureCapacity(codecs.length + 1);
                    // Add first.
                    add(codec);
                    addAll(InternalArrays.asImmutableList(codecs));
                } else {
                    add(0, codec);
                }
            }
            return this;
        }

        @Override
        public CodecsBuilder addLast(Codec<?> codec) {
            synchronized (this) {
                if (isEmpty()) {
                    addAll(InternalArrays.asImmutableList(defaultCodecs(allocator)));
                }
                add(codec);
            }
            return this;
        }

        @Override
        public Codecs build() {
            synchronized (this) {
                try {
                    if (isEmpty()) {
                        return new DefaultCodecs(defaultCodecs(allocator));
                    }
                    return new DefaultCodecs(toArray(new Codec<?>[0]));
                } finally {
                    clear();
                    trimToSize();
                }
            }
        }
    }
}
