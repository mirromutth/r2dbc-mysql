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

import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import org.testcontainers.shaded.com.google.common.base.CaseFormat;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link SetCodec}.
 */
class SetCodecTest implements CodecTestSupport<Set<?>, NormalFieldValue, ParameterizedType> {

    private final Set<?>[] sets = {
        EnumSet.allOf(ToStr.class),
        EnumSet.noneOf(ToStr.class),
        EnumSet.allOf(CaseFormat.class),
        EnumSet.of(CaseFormat.LOWER_UNDERSCORE, CaseFormat.LOWER_HYPHEN),
        Collections.emptySet(),
        Collections.singleton(""),
        Collections.singleton(ToStr.GOOD),
        Collections.singleton("\r\n\0\032\\'\"\u00a5\u20a9"),
        new HashSet<>(Arrays.asList("Hello", "world!")),
        new HashSet<>(Arrays.asList("", "")),
        new HashSet<>(Arrays.asList(ToStr.GOOD, ToStr.NICE)),
        new HashSet<>(Arrays.asList("Hello", "R2DBC", "MySQL")),
    };

    @Override
    public SetCodec getCodec() {
        return SetCodec.INSTANCE;
    }

    @Override
    public Set<?>[] originParameters() {
        return sets;
    }

    @Override
    public Object[] stringifyParameters() {
        String[] results = new String[sets.length];
        for (int i = 0; i < results.length; ++i) {
            String value = sets[i].stream()
                .map(SetCodec::convert)
                .map(ESCAPER::escape)
                .collect(Collectors.joining(","));
            results[i] = String.format("'%s'", value);
        }
        return results;
    }

    private enum ToStr {

        GOOD {

            @Override
            public String toString() {
                throw new IllegalStateException("Special enum class, can not to string");
            }
        },

        NICE {

            @Override
            public String toString() {
                throw new IllegalStateException("Special enum class, can not to string");
            }
        },
    }
}
