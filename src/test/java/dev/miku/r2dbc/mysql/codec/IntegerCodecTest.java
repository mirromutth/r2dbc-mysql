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

/**
 * Unit tests for {@link IntegerCodec}.
 */
class IntegerCodecTest implements CodecTestSupport<Integer, NormalFieldValue, Class<? super Integer>> {

    private final Integer[] integers = {
        0,
        1,
        -1,
        10,
        -10,
        (int) Short.MAX_VALUE,
        (int) Short.MIN_VALUE,
        Integer.MAX_VALUE,
        Integer.MIN_VALUE,
    };

    @Override
    public IntegerCodec getCodec() {
        return IntegerCodec.INSTANCE;
    }

    @Override
    public Integer[] originParameters() {
        return integers;
    }

    @Override
    public Object[] stringifyParameters() {
        return integers;
    }
}
