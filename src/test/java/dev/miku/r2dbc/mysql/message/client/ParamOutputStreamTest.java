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

package dev.miku.r2dbc.mysql.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit tests for {@link ParamOutputStream}.
 */
class ParamOutputStreamTest {

    private static final int SIZE = 10;

    @Test
    void publishSuccess() {
        ByteBuf buf = Unpooled.buffer();
        MockParameter[] values = new MockParameter[SIZE];

        for (int i = 0; i < SIZE; ++i) {
            values[i] = new MockParameter(true);
        }

        Flux.from(ParamOutputStream.publish(buf, values))
            .doOnNext(pubBuf -> assertFalse(pubBuf.isReadable()))
            .map(ByteBuf::release)
            .reduce(Boolean::logicalAnd)
            .as(StepVerifier::create)
            .expectNext(Boolean.TRUE)
            .verifyComplete();

        assertEquals(buf.refCnt(), 0);
        assertThat(values).extracting(MockParameter::refCnt).containsOnly(0);
    }

    @Test
    void publishPartially() {
        ByteBuf buf = Unpooled.buffer();
        MockParameter[] values = new MockParameter[SIZE];

        int i = 0;

        for (; i < SIZE >>> 1; ++i) {
            values[i] = new MockParameter(true);
        }

        for (; i < SIZE; ++i) {
            values[i] = new MockParameter(false);
        }

        Flux.from(ParamOutputStream.publish(buf, values))
            .as(StepVerifier::create)
            .verifyError(MockException.class);

        assertEquals(buf.refCnt(), 0);
        assertThat(values).extracting(MockParameter::refCnt).containsOnly(0);
    }

    @Test
    void publishNothing() {
        ByteBuf buf = Unpooled.buffer();
        MockParameter[] values = new MockParameter[SIZE];

        for (int i = 0; i < SIZE; ++i) {
            values[i] = new MockParameter(false);
        }

        Flux.from(ParamOutputStream.publish(buf, values))
            .as(StepVerifier::create)
            .verifyError(MockException.class);

        assertEquals(buf.refCnt(), 0);
        assertThat(values).extracting(MockParameter::refCnt).containsOnly(0);
    }
}
