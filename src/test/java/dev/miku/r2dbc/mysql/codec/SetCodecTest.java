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
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.message.client.ParameterWriterHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.base.CaseFormat;
import reactor.core.publisher.Flux;
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
    public SetCodec getCodec(ByteBufAllocator allocator) {
        return new SetCodec(allocator);
    }

    @Override
    public String[][] originParameters() {
        return strings;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(strings)
            .map(it -> Arrays.stream(it)
                .map(ESCAPER::escape)
                .collect(Collectors.joining(",", "'", "'")))
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
    void binarySet() {
        SetCodec codec = getCodec(UnpooledByteBufAllocator.DEFAULT);
        ByteBuf[] binaries = binarySets(CharCollation.clientCharCollation().getCharset());

        assertEquals(sets.length, binaries.length);

        for (int i = 0; i < sets.length; ++i) {
            merge(Flux.from(codec.encode(sets[i], context()).publishBinary()))
                .as(StepVerifier::create)
                .expectNext(sized(binaries[i]))
                .verifyComplete();
        }
    }

    @Test
    void stringifySet() {
        SetCodec codec = getCodec(UnpooledByteBufAllocator.DEFAULT);
        String[] strings = stringifySets();

        assertEquals(sets.length, strings.length);

        for (int i = 0; i < sets.length; ++i) {
            ParameterWriter writer = ParameterWriterHelper.get(1);
            codec.encode(sets[i], context())
                .publishText(writer)
                .as(StepVerifier::create)
                .verifyComplete();
            assertEquals(ParameterWriterHelper.toSql(writer), strings[i]);
        }
    }

    private String[] stringifySets() {
        return Arrays.stream(sets)
            .map(set -> set.stream()
                .map(it -> it instanceof Enum<?> ? ((Enum<?>) it).name() : it.toString())
                .map(ESCAPER::escape)
                .collect(Collectors.joining(",", "'", "'")))
            .toArray(String[]::new);
    }

    private ByteBuf[] binarySets(Charset charset) {
        return Arrays.stream(sets)
            .map(set -> set.stream()
                .map(it -> it instanceof Enum<?> ? ((Enum<?>) it).name() : it.toString())
                .collect(Collectors.joining(","))
                .getBytes(charset))
            .map(Unpooled::wrappedBuffer)
            .toArray(ByteBuf[]::new);
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
