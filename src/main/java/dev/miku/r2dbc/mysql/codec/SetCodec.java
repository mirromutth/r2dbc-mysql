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
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.util.InternalArrays;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.lang.reflect.ParameterizedType;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_STRINGS;

/**
 * Codec for {@link Set}{@code <}{@link String}{@code >}, {@link Set}{@code <}{@link Enum}{@code >} and the
 * {@link String}{@code []}.
 */
final class SetCodec implements ParametrizedCodec<String[]> {

    private final ByteBufAllocator allocator;

    SetCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public String[] decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary,
        CodecContext context) {
        if (!value.isReadable()) {
            return EMPTY_STRINGS;
        }

        int firstComma = value.indexOf(value.readerIndex(), value.writerIndex(), (byte) ',');
        Charset charset = CharCollation.fromId(info.getCollationId(), context.getServerVersion())
            .getCharset();

        if (firstComma < 0) {
            return new String[] { value.toString(charset) };
        }

        return value.toString(charset).split(",");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Set<?> decode(ByteBuf value, FieldInformation info, ParameterizedType target, boolean binary,
        CodecContext context) {
        if (!value.isReadable()) {
            return Collections.emptySet();
        }

        Class<?> subClass = (Class<?>) target.getActualTypeArguments()[0];
        Charset charset = CharCollation.fromId(info.getCollationId(), context.getServerVersion())
            .getCharset();
        int firstComma = value.indexOf(value.readerIndex(), value.writerIndex(), (byte) ',');
        boolean isEnum = subClass.isEnum();

        if (firstComma < 0) {
            if (isEnum) {
                return Collections.singleton(Enum.valueOf((Class<Enum>) subClass, value.toString(charset)));
            }

            return Collections.singleton(value.toString(charset));
        }

        Iterable<String> elements = new SplitIterable(value, charset, firstComma);
        Set<?> result = buildSet(subClass, isEnum);

        if (isEnum) {
            Class<Enum> enumClass = (Class<Enum>) subClass;
            Set<Enum<?>> enumSet = (Set<Enum<?>>) result;
            for (String element : elements) {
                enumSet.add(Enum.valueOf(enumClass, element));
            }
        } else {
            for (String element : elements) {
                ((Set<String>) result).add(element);
            }
        }

        return result;
    }

    @Override
    public boolean canDecode(FieldInformation info, Class<?> target) {
        return DataTypes.SET == info.getType() && target.isAssignableFrom(String[].class);
    }

    @Override
    public boolean canDecode(FieldInformation info, ParameterizedType target) {
        if (DataTypes.SET != info.getType()) {
            return false;
        }

        Class<?> argument = ParametrizedUtils.getTypeArgument(target, Set.class);

        return argument != null && (argument.isEnum() || argument.isAssignableFrom(String.class));
    }

    @Override
    public boolean canEncode(Object value) {
        return (value instanceof CharSequence[]) || (value instanceof Set<?> && isValidSet((Set<?>) value));
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        if (value instanceof CharSequence[]) {
            return new StringArrayParameter(allocator, InternalArrays.toImmutableList((CharSequence[]) value),
                context);
        }

        return new SetParameter(allocator, (Set<?>) value, context);
    }

    private static Set<?> buildSet(Class<?> subClass, boolean isEnum) {
        if (isEnum) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            EnumSet<?> s = EnumSet.noneOf((Class<Enum>) subClass);
            return s;
        }

        return new LinkedHashSet<String>();
    }

    private static boolean isValidSet(Set<?> value) {
        for (Object element : value) {
            if (!(element instanceof CharSequence) && !(element instanceof Enum<?>)) {
                return false;
            }
        }

        return true;
    }

    private static void encodeIterator(ParameterWriter writer, Iterator<? extends CharSequence> iter) {
        if (iter.hasNext()) {
            writer.append(iter.next());

            while (iter.hasNext()) {
                writer.append(',').append(iter.next());
            }
        } else {
            // Empty set, set to string mode.
            writer.startString();
        }
    }

    private static ByteBuf encodeSet(ByteBufAllocator alloc, Iterator<? extends CharSequence> iter,
        CodecContext context) {
        Charset charset = context.getClientCollation().getCharset();
        ByteBuf content = alloc.buffer();

        try {
            // Max size of var int, fill zero to protect memory data.
            VarIntUtils.reserveVarInt(content);

            CharSequence name = iter.next();
            int size = content.writeCharSequence(name, charset);

            while (iter.hasNext()) {
                name = iter.next();
                size += content.writeByte(',').writeCharSequence(name, charset) + 1;
            }

            return VarIntUtils.setReservedVarInt(content, size);
        } catch (Throwable e) {
            content.release();
            throw e;
        }
    }

    private static final class SplitIterable implements Iterable<String> {

        private final ByteBuf buf;

        private final Charset charset;

        private final int firstComma;

        SplitIterable(ByteBuf buf, Charset charset, int firstComma) {
            this.buf = buf;
            this.charset = charset;

            if (firstComma < 0) {
                this.firstComma = buf.writerIndex();
            } else {
                this.firstComma = firstComma;
            }
        }

        @Override
        public Iterator<String> iterator() {
            return new SplitIterator(buf, charset, firstComma);
        }
    }

    private static final class SplitIterator implements Iterator<String> {

        private final ByteBuf buf;

        private final Charset charset;

        private int lastChar;

        private int currentComma;

        private final int writerIndex;

        SplitIterator(ByteBuf buf, Charset charset, int currentComma) {
            this.buf = buf;
            this.charset = charset;
            this.lastChar = buf.readerIndex();
            this.currentComma = currentComma;
            this.writerIndex = buf.writerIndex();
        }

        @Override
        public boolean hasNext() {
            return currentComma <= writerIndex && currentComma >= lastChar;
        }

        @Override
        public String next() {
            String result = buf.toString(lastChar, currentComma - lastChar, charset);
            int nextStart = currentComma + 1;

            lastChar = nextStart;
            currentComma = nextComma(nextStart);

            return result;
        }

        private int nextComma(int nextStart) {
            if (nextStart > writerIndex) {
                return nextStart;
            }

            int index = buf.indexOf(nextStart, writerIndex, (byte) ',');

            if (index < 0) {
                return writerIndex;
            }

            return index;
        }
    }

    private static final class ConvertedIterator implements Iterator<String> {

        private final Iterator<?> origin;

        private ConvertedIterator(Iterator<?> origin) {
            this.origin = origin;
        }

        @Override
        public boolean hasNext() {
            return origin.hasNext();
        }

        @Override
        public String next() {
            Object o = origin.next();
            return o instanceof Enum<?> ? ((Enum<?>) o).name() : o.toString();
        }
    }

    private static final class SetParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final Set<?> value;

        private final CodecContext context;

        private SetParameter(ByteBufAllocator allocator, Set<?> value, CodecContext context) {
            this.allocator = allocator;
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> {
                if (value.isEmpty()) {
                    // It is zero of var int, not terminal.
                    return allocator.buffer(Byte.BYTES).writeByte(0);
                }

                return encodeSet(allocator, new ConvertedIterator(value.iterator()), context);
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> encodeIterator(writer, new ConvertedIterator(value.iterator())));
        }

        @Override
        public short getType() {
            return DataTypes.VARCHAR;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SetParameter)) {
                return false;
            }

            SetParameter setValue = (SetParameter) o;

            return value.equals(setValue.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    private static final class StringArrayParameter extends AbstractParameter {

        private final ByteBufAllocator allocator;

        private final List<CharSequence> value;

        private final CodecContext context;

        private StringArrayParameter(ByteBufAllocator allocator, List<CharSequence> value,
            CodecContext context) {
            this.allocator = allocator;
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> {
                if (value.isEmpty()) {
                    // It is zero of var int, not terminal.
                    return allocator.buffer(Byte.BYTES).writeByte(0);
                }

                return encodeSet(allocator, value.iterator(), context);
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> encodeIterator(writer, value.iterator()));
        }

        @Override
        public short getType() {
            return DataTypes.VARCHAR;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StringArrayParameter)) {
                return false;
            }

            StringArrayParameter that = (StringArrayParameter) o;

            return this.value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
