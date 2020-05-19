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

import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import dev.miku.r2dbc.mysql.ConnectionContext;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.escape.Escaper;
import org.testcontainers.shaded.com.google.common.escape.Escapers;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class considers unit tests for implementations of {@link Codec}.
 */
interface CodecTestSupport<T> {

    ConnectionContext CONTEXT = new ConnectionContext(ZeroDateOption.USE_NULL);

    Escaper ESCAPER = Escapers.builder()
        .addEscape('\'', "''")
        .addEscape('\0', "\\0")
        .addEscape('\r', "\\r")
        .addEscape('\n', "\\n")
        .addEscape('\\', "\\\\")
        .addEscape('\032', "\\Z")
        .build();

    @Test
    default void stringify() {
        Codec<T> codec = getCodec();
        T[] origin = originParameters();
        Object[] strings = stringifyParameters();

        assertEquals(origin.length, strings.length);

        for (int i = 0; i < origin.length; ++i) {
            StringBuilder builder = new StringBuilder();
            codec.encode(origin[i], CONTEXT)
                .writeTo(builder)
                .as(StepVerifier::create)
                .verifyComplete();
            assertEquals(builder.toString(), strings[i].toString());
        }
    }

    Codec<T> getCodec();

    T[] originParameters();

    Object[] stringifyParameters();
}
