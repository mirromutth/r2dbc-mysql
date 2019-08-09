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

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.DataTypes;
import io.github.mirromutth.r2dbc.mysql.internal.ConnectionContext;
import io.github.mirromutth.r2dbc.mysql.message.FieldValue;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Codec for {@link Set<String>} or {@link Set<Enum>}.
 */
final class SetCodec implements Codec<Set<?>, NormalFieldValue, ParameterizedType> {

    static final SetCodec INSTANCE = new SetCodec();

    private SetCodec() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<?> decode(NormalFieldValue value, FieldInformation info, ParameterizedType target, boolean binary, ConnectionContext context) {
        ByteBuf buf = value.getBufferSlice();

        if (!buf.isReadable()) {
            return Collections.emptySet();
        }

        Class<?> subClass = (Class<?>) target.getActualTypeArguments()[0];
        Charset charset = CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset();
        int firstComma = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) ',');

        if (firstComma < 0) {
            if (subClass.isEnum()) {
                return Collections.singleton(Enum.valueOf((Class<Enum>) subClass, buf.toString(charset)));
            } else {
                return Collections.singleton(buf.toString(charset));
            }
        }

        Iterable<String> elements = new SplitIterable(buf, charset, firstComma);
        Set<?> result = buildSet(subClass);

        if (subClass.isEnum()) {
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
    public boolean canDecode(FieldValue value, FieldInformation info, Type target) {
        if (DataTypes.SET != info.getType() || !(target instanceof ParameterizedType) || !(value instanceof NormalFieldValue)) {
            return false;
        }

        ParameterizedType parameterizedType = (ParameterizedType) target;
        Type[] typeArguments = parameterizedType.getActualTypeArguments();

        if (typeArguments.length != 1) {
            return false;
        }

        Type rawType = parameterizedType.getRawType();
        Type subType = typeArguments[0];

        if (!(rawType instanceof Class<?>) || !(subType instanceof Class<?>)) {
            return false;
        }

        Class<?> rawClass = (Class<?>) rawType;
        Class<?> subClass = (Class<?>) subType;

        return rawClass.isAssignableFrom(Set.class) &&
            (subClass.isEnum() || subClass.isAssignableFrom(String.class));
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Set<?> && isValidSet((Set<?>) value);
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new SetValue((Set<?>) value, context);
    }

    private static Set<?> buildSet(Class<?> subClass) {
        if (subClass.isEnum()) {
            @SuppressWarnings("unchecked")
            EnumSet<?> s = EnumSet.noneOf((Class<Enum>) subClass);
            return s;
        }

        return new LinkedHashSet<String>();
    }

    private static boolean isValidSet(Set<?> value) {
        for (Object element : value) {
            if (element == null || (!(element instanceof CharSequence) && !element.getClass().isEnum())) {
                return false;
            }
        }

        return true;
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

    private static final class SetValue extends AbstractParameterValue {

        private static final Function<Object, CharSequence> ELEMENT_CONVERT = element -> {
            if (element.getClass().isEnum()) {
                return ((Enum<?>) element).name();
            } else {
                return element.toString();
            }
        };

        private final Set<?> set;

        private final ConnectionContext context;

        private SetValue(Set<?> set, ConnectionContext context) {
            this.set = set;
            this.context = context;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Flux.fromIterable(set)
                .map(ELEMENT_CONVERT)
                .collectList()
                .doOnNext(strings -> writer.writeSet(strings, context.getCollation()))
                .then();
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
            if (!(o instanceof SetValue)) {
                return false;
            }

            SetValue setValue = (SetValue) o;

            return set.equals(setValue.set);
        }

        @Override
        public int hashCode() {
            return set.hashCode();
        }
    }
}
