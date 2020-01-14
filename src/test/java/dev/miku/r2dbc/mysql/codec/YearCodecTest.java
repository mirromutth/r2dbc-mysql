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

import java.time.Year;

/**
 * Unit tests for {@link YearCodec}.
 */
class YearCodecTest implements CodecTestSupport<Year, NormalFieldValue, Class<? super Year>> {

    private final Year[] years = {
        Year.of(0),
        Year.of(1000),
        Year.of(1900),
        Year.of(2100),
        // Following should not be permitted by MySQL server, but also test.
        Year.of(Year.MAX_VALUE),
        Year.of(Year.MIN_VALUE),
    };

    @Override
    public YearCodec getCodec() {
        return YearCodec.INSTANCE;
    }

    @Override
    public Year[] originParameters() {
        return years;
    }

    @Override
    public Object[] stringifyParameters() {
        return years;
    }
}
