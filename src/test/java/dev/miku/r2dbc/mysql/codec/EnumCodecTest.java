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

import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import org.testcontainers.shaded.com.google.common.base.CaseFormat;
import org.testcontainers.shaded.com.google.common.collect.BoundType;

/**
 * Unit tests for {@link EnumCodec}.
 */
class EnumCodecTest implements CodecTestSupport<Enum<?>, NormalFieldValue, Class<?>> {

    private final Enum<?>[] enums = {
        // Java has no way to create an element of enum with special character.
        // Maybe imports other languages here?
        SomeElement.A1B2,
        BoundType.OPEN,
        BoundType.CLOSED,
        CaseFormat.LOWER_CAMEL,
        SomeElement.$$$$,
    };

    @Override
    public EnumCodec getCodec() {
        return EnumCodec.INSTANCE;
    }

    @Override
    public Enum<?>[] originParameters() {
        return enums;
    }

    @Override
    public Object[] stringifyParameters() {
        String[] results = new String[enums.length];
        for (int i = 0; i < results.length; ++i) {
            results[i] = String.format("'%s'", ESCAPER.escape(enums[i].name()));
        }
        return results;
    }

    private enum SomeElement {

        $$$$ {

            @Override
            public String toString() {
                throw new IllegalStateException("Special enum class, can not to string");
            }
        },

        A1B2 {

            @Override
            public String toString() {
                throw new IllegalStateException("Special enum class, can not to string");
            }
        },
    }
}
