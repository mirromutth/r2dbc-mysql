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

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link StringArrayCodec}.
 */
class StringArrayCodecTest implements CodecTestSupport<String[], NormalFieldValue, Class<? super String[]>> {

    private final String[][] strings = {
        {},
        {""},
        {"\r\n\0\032\\'\"\u00a5\u20a9"},
        {"Hello", "world!"},
        {"", ""},
        {"Hello", "R2DBC", "MySQL"},
    };

    @Override
    public StringArrayCodec getCodec() {
        return StringArrayCodec.INSTANCE;
    }

    @Override
    public String[][] originParameters() {
        return strings;
    }

    @Override
    public Object[] stringifyParameters() {
        String[] results = new String[strings.length];
        for (int i = 0; i < results.length; ++i) {
            String value = Arrays.stream(strings[i])
                .map(ESCAPER::escape)
                .collect(Collectors.joining(","));
            results[i] = String.format("'%s'", value);
        }
        return results;
    }
}
