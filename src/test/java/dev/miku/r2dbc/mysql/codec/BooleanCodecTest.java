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

/**
 * Unit tests for {@link BooleanCodec}.
 */
class BooleanCodecTest implements CodecTestSupport<Boolean, NormalFieldValue, Class<? super Boolean>> {

    private final Boolean[] booleans = {
        Boolean.TRUE,
        Boolean.FALSE
    };

    @Override
    public BooleanCodec getCodec() {
        return BooleanCodec.INSTANCE;
    }

    @Override
    public Boolean[] originParameters() {
        return booleans;
    }

    @Override
    public Object[] stringifyParameters() {
        String[] results = new String[booleans.length];
        for (int i = 0; i < results.length; ++i) {
            results[i] = booleans[i] ? "b'1'" : "b'0'";
        }
        return results;
    }
}
