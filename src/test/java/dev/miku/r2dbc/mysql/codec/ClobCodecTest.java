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

import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Unit tests for {@link ClobCodec}.
 */
class ClobCodecTest implements CodecTestSupport<Clob> {

    private final MockClob[] clob = {
        new MockClob(""),
        new MockClob("", ""),
        new MockClob("Hello, world!"),
        new MockClob("\r\n\0\032\\'\"\u00a5\u20a9"),
        new MockClob("Hello, ", "R2DBC", "!"),
        new MockClob("", "Hi, ", "MySQL"),
        new MockClob("Hi, ", "", "MySQL"),
        new MockClob("Hi, ", "MySQL", ""),
    };

    @Override
    public ClobCodec getCodec() {
        return ClobCodec.INSTANCE;
    }

    @Override
    public Clob[] originParameters() {
        return clob;
    }

    @Override
    public Object[] stringifyParameters() {
        String[] results = new String[clob.length];
        for (int i = 0; i < results.length; ++i) {
            results[i] = String.format("'%s'", ESCAPER.escape(String.join("", clob[i].values)));
        }
        return results;
    }

    private static final class MockClob implements Clob {

        private final String[] values;

        private MockClob(String ...values) {
            this.values = values;
        }

        @Override
        public Flux<CharSequence> stream() {
            return Flux.fromArray(values);
        }

        @Override
        public Mono<Void> discard() {
            return Mono.empty();
        }
    }
}
