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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit tests for {@link ParameterWriter}.
 */
class ParameterWriterTest {

    private static final int SIZE = 10;

    @Test
    void publishSuccess() {
        ByteBuf buf = Unpooled.buffer();
        MockValue[] values = new MockValue[SIZE];

        for (int i = 0; i < SIZE; ++i) {
            values[i] = new MockValue(true);
        }

        Flux.from(ParameterWriter.publish(buf, values))
            .doOnNext(pubBuf -> assertFalse(pubBuf.isReadable()))
            .map(ByteBuf::release)
            .reduce(Boolean::logicalAnd)
            .as(StepVerifier::create)
            .expectNext(Boolean.TRUE)
            .verifyComplete();

        assertEquals(buf.refCnt(), 0);
        assertThat(values).extracting(MockValue::refCnt).containsOnly(0);
    }

    @Test
    void publishPartially() {
        ByteBuf buf = Unpooled.buffer();
        MockValue[] values = new MockValue[SIZE];

        int i = 0;

        for (; i < SIZE >>> 1; ++i) {
            values[i] = new MockValue(true);
        }

        for (; i < SIZE; ++i) {
            values[i] = new MockValue(false);
        }

        Flux.from(ParameterWriter.publish(buf, values))
            .as(StepVerifier::create)
            .verifyError(MockException.class);

        assertEquals(buf.refCnt(), 0);
        assertThat(values).extracting(MockValue::refCnt).containsOnly(0);
    }

    @Test
    void publishNothing() {
        ByteBuf buf = Unpooled.buffer();
        MockValue[] values = new MockValue[SIZE];

        for (int i = 0; i < SIZE; ++i) {
            values[i] = new MockValue(false);
        }

        Flux.from(ParameterWriter.publish(buf, values))
            .as(StepVerifier::create)
            .verifyError(MockException.class);

        assertEquals(buf.refCnt(), 0);
        assertThat(values).extracting(MockValue::refCnt).containsOnly(0);
    }

    private static final class MockException extends RuntimeException {

        private static final MockException INSTANCE = new MockException();

        private MockException() {
            super("Mocked exception");
        }
    }

    private static final class MockValue extends AtomicInteger implements ParameterValue {

        private final boolean success;

        MockValue(boolean success) {
            super(1);
            this.success = success;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            if (success) {
                return Mono.fromRunnable(this::dispose);
            } else {
                return Mono.error(() -> {
                    dispose();
                    return MockException.INSTANCE;
                });
            }
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            if (success) {
                return Mono.fromRunnable(this::dispose);
            } else {
                return Mono.error(() -> {
                    dispose();
                    return MockException.INSTANCE;
                });
            }
        }

        @Override
        public short getType() {
            return DataTypes.TIME;
        }

        @Override
        public void dispose() {
            int refCnt;

            do {
                refCnt = get();

                if (refCnt <= 0) {
                    throw new IllegalReferenceCountException(refCnt);
                }
            } while (!compareAndSet(refCnt, refCnt - 1));
        }

        int refCnt() {
            return get();
        }
    }
}
