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

package dev.miku.r2dbc.mysql.codec.lob;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link LobUtils}.
 */
class LobUtilsTest {

    private static final String SINGLE = "Hello world";

    private static final String[] MULTI = { "Hello", "R2DBC", "MySQL" };

    @Test
    void normalFieldValue() {
        ByteBuf buf = createSingle();

        Flux.from(LobUtils.createBlob(buf).stream())
            .as(StepVerifier::create)
            .expectNext(ByteBuffer.wrap(SINGLE.getBytes()))
            .verifyComplete();

        // Mock row releasing.
        buf.release();

        assertEquals(buf.refCnt(), 0);
    }

    @Test
    void largeFieldValue() {
        List<ByteBuf> buffers = createMulti();

        Flux.from(LobUtils.createBlob(buffers).stream())
            .as(StepVerifier::create)
            .expectNext(ByteBuffer.wrap(MULTI[0].getBytes()), ByteBuffer.wrap(MULTI[1].getBytes()),
                ByteBuffer.wrap(MULTI[2].getBytes()))
            .verifyComplete();

        // Mock row releasing.
        for (ByteBuf buf : buffers) {
            buf.release();
        }

        assertThat(buffers).extracting(ByteBuf::refCnt).containsOnly(0);
    }

    @Test
    void consumePortion() {
        List<ByteBuf> buffers = createMulti();

        Flux.from(LobUtils.createBlob(buffers).stream())
            .as(it -> StepVerifier.create(it, 2))
            .expectNext(ByteBuffer.wrap(MULTI[0].getBytes()), ByteBuffer.wrap(MULTI[1].getBytes()))
            .thenCancel()
            .verify();

        // Mock row releasing.
        for (ByteBuf buf : buffers) {
            buf.release();
        }

        assertThat(buffers).extracting(ByteBuf::refCnt).containsOnly(0);
    }

    private static ByteBuf createSingle() {
        return Unpooled.wrappedBuffer(SINGLE.getBytes());
    }

    private static List<ByteBuf> createMulti() {
        return Stream.of(MULTI).map(it -> Unpooled.wrappedBuffer(it.getBytes())).collect(Collectors.toList());
    }
}
