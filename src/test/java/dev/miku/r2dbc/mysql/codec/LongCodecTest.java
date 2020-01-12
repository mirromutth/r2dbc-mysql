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
 * Unit tests for {@link LongCodec}.
 */
class LongCodecTest implements CodecTestSupport<Long, NormalFieldValue, Class<? super Long>> {

    private final Long[] longs = {
        0L,
        1L,
        -1L,
        10L,
        -10L,
        (long) Integer.MIN_VALUE,
        (long) Integer.MAX_VALUE,
        Long.MIN_VALUE,
        Long.MAX_VALUE
    };

    @Override
    public LongCodec getCodec() {
        return LongCodec.INSTANCE;
    }

    @Override
    public Long[] originParameters() {
        return longs;
    }

    @Override
    public Object[] stringifyParameters() {
        return longs;
    }
}
