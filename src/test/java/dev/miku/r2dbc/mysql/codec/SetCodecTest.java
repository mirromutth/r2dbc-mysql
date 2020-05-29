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

import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.message.client.ParameterWriterHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.base.CaseFormat;
import reactor.test.StepVerifier;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link SetCodec}.
 */
class SetCodecTest implements CodecTestSupport<String[]> {

    private final String[][] strings = {
        {},
        {""},
        {"\r\n\0\032\\'\"\u00a5\u20a9"},
        {"Hello", "world!"},
        {"", ""},
        {"Hello", "R2DBC", "MySQL"},
    };

    private final Set<?>[] sets = {
        EnumSet.allOf(SomeElement.class),
        EnumSet.noneOf(SomeElement.class),
        EnumSet.allOf(CaseFormat.class),
        EnumSet.of(CaseFormat.LOWER_UNDERSCORE, CaseFormat.LOWER_HYPHEN),
        Collections.emptySet(),
        Collections.singleton(""),
        Collections.singleton(SomeElement.GOOD),
        Collections.singleton("\r\n\0\032\\'\"\u00a5\u20a9"),
        new HashSet<>(Arrays.asList("Hello", "world!")),
        new HashSet<>(Arrays.asList("", "")),
        new HashSet<>(Arrays.asList(SomeElement.GOOD, SomeElement.NICE)),
        new HashSet<>(Arrays.asList("Hello", "R2DBC", "MySQL")),
    };

    @Override
    public SetCodec getCodec() {
        return SetCodec.INSTANCE;
    }

    @Override
    public String[][] originParameters() {
        return strings;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(strings)
            .map(it -> String.format("'%s'", Arrays.stream(it)
                .map(ESCAPER::escape)
                .collect(Collectors.joining(","))))
            .toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(strings)
            .map(it -> String.join(",", it).getBytes(charset))
            .map(Unpooled::wrappedBuffer)
            .toArray(ByteBuf[]::new);
    }

    @Test
    void stringifySet() {
        SetCodec codec = getCodec();
        String[] strings = stringifySets();

        assertEquals(sets.length, strings.length);

        for (int i = 0; i < sets.length; ++i) {
            ParameterWriter writer = ParameterWriterHelper.get(1);
            codec.encode(sets[i], CONTEXT)
                .text(writer)
                .as(StepVerifier::create)
                .verifyComplete();
            assertEquals(ParameterWriterHelper.toSql(writer), strings[i]);
        }
    }

    private String[] stringifySets() {
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

    private enum SomeElement {

        GOOD,
        NICE;

        @Override
        public final String toString() {
            throw new IllegalStateException("Special enum class, can not to string");
        }
    }
}
