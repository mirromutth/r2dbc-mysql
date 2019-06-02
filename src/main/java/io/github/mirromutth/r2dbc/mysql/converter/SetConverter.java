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

package io.github.mirromutth.r2dbc.mysql.converter;

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.ColumnType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Converter for {@link Set<String>} or {@link Set<Enum>}.
 */
final class SetConverter implements Converter<Set<?>, ParameterizedType> {

    static final SetConverter INSTANCE = new SetConverter();

    private SetConverter() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<?> read(ByteBuf buf, short definitions, int precision, int collationId, ParameterizedType target, MySqlSession session) {
        Class<?> rawClass = (Class<?>) target.getRawType();
        Class<?> subClass = (Class<?>) target.getActualTypeArguments()[0];
        Set<?> result;

        try {
            result = buildSet(rawClass, subClass);
        } catch (InstantiationException | IllegalAccessException | ClassCastException e) {
            throw new IllegalStateException("can not build " + rawClass + " without args of a java.util.Set implementation", e);
        }

        String[] allElements = buf.toString(CharCollation.fromId(collationId, session.getServerVersion()).getCharset()).split(",");

        if (subClass.isEnum()) {
            Class<Enum> enumClass = (Class<Enum>) subClass;
            Set<Enum<?>> enumSet = (Set<Enum<?>>) result;
            for (String element : allElements) {
                enumSet.add(Enum.valueOf(enumClass, element));
            }
        } else {
            ((Set<String>) result).addAll(Arrays.asList(allElements));
        }

        return result;
    }

    @Override
    public boolean canRead(ColumnType type, short definitions, int precision, int collationId, Type target, MySqlSession session) {
        if (ColumnType.SET != type || !(target instanceof ParameterizedType)) {
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

        if (subClass.isEnum()) {
            // main set type is EnumSet
            return rawClass.isAssignableFrom(EnumSet.class) || Set.class.isAssignableFrom(rawClass);
        } else if (subClass.isAssignableFrom(String.class)) {
            // main set type is LinkedHashSet
            return rawClass.isAssignableFrom(LinkedHashSet.class) || Set.class.isAssignableFrom(rawClass);
        }

        return false;
    }

    private Set<?> buildSet(Class<?> rawClass, Class<?> subClass) throws IllegalAccessException, InstantiationException {
        if (subClass.isEnum()) {
            if (rawClass.isAssignableFrom(EnumSet.class)) {
                @SuppressWarnings("unchecked")
                EnumSet<?> s = EnumSet.noneOf((Class<Enum>) subClass);
                return s;
            }
        } else if (rawClass.isAssignableFrom(LinkedHashSet.class)) {
            return new LinkedHashSet<String>();
        }

        return (Set<?>) rawClass.newInstance();
    }
}
