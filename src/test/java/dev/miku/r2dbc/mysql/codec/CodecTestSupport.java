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

import dev.miku.r2dbc.mysql.ConnectionContextTest;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.Query;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.message.client.ParameterWriterHelper;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.escape.Escaper;
import org.testcontainers.shaded.com.google.common.escape.Escapers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.Charset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class considers unit tests for implementations of {@link Codec}.
 * <p>
 * TODO: add test cases for decoding.
 */
interface CodecTestSupport<T> {

    Escaper ESCAPER = Escapers.builder()
        .addEscape('\'', "''")
        .addEscape('\0', "\\0")
        .addEscape('\r', "\\r")
        .addEscape('\n', "\\n")
        .addEscape('\\', "\\\\")
        .addEscape('\032', "\\Z")
        .build();

    @Test
    default void binary() {
        Codec<T> codec = getCodec(UnpooledByteBufAllocator.DEFAULT);
        T[] origin = originParameters();
        ByteBuf[] binaries = binaryParameters(CharCollation.clientCharCollation().getCharset());

        assertThat(origin).hasSize(binaries.length);

        for (int i = 0; i < origin.length; ++i) {
            AtomicReference<ByteBuf> buf = new AtomicReference<>();
            ByteBuf sized = sized(binaries[i]);
            try {
                merge(Flux.from(codec.encode(origin[i], context()).publishBinary()))
                    .doOnNext(buf::set)
                    .as(StepVerifier::create)
                    .expectNext(sized)
                    .verifyComplete();
            } finally {
                sized.release();
                Optional.ofNullable(buf.get()).ifPresent(ByteBuf::release);
            }
        }
    }

    @Test
    default void stringify() {
        Codec<T> codec = getCodec(UnpooledByteBufAllocator.DEFAULT);
        T[] origin = originParameters();
        Object[] strings = stringifyParameters();

        assertThat(origin).hasSize(strings.length);

        Query query = Query.parse("?");

        for (int i = 0; i < origin.length; ++i) {
            ParameterWriter writer = ParameterWriterHelper.get(query);
            codec.encode(origin[i], context())
                .publishText(writer)
                .as(StepVerifier::create)
                .verifyComplete();
            assertEquals(ParameterWriterHelper.toSql(writer), strings[i].toString());
        }
    }

    /**
     * If encoding no need sized, override it and just return origin value.
     */
    default ByteBuf sized(ByteBuf value) {
        ByteBuf buf = Unpooled.buffer();
        try {
            VarIntUtils.writeVarInt(buf, value.readableBytes());
            return buf.writeBytes(value);
        } catch (Throwable e) {
            buf.release();
            throw e;
        } finally {
            value.release();
        }
    }

    default Mono<ByteBuf> merge(Flux<ByteBuf> buffers) {
        return Mono.create(sink -> {
            ByteBuf buf = Unpooled.buffer();
            buffers.subscribe(buffer -> {
                try {
                    buf.writeBytes(buffer);
                } finally {
                    buffer.release();
                }
            }, sink::error, () -> {
                if (buf.isReadable()) {
                    sink.success(buf);
                } else {
                    sink.error(new IllegalArgumentException("Encoded but nothing received"));
                }
            });
        });
    }

    default CodecContext context() {
        return ConnectionContextTest.mock();
    }

    Codec<T> getCodec(ByteBufAllocator allocator);

    T[] originParameters();

    Object[] stringifyParameters();

    ByteBuf[] binaryParameters(Charset charset);
}
