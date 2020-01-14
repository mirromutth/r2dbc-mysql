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

import dev.miku.r2dbc.mysql.message.FieldValue;
import io.r2dbc.spi.Blob;
import org.testcontainers.shaded.org.apache.commons.codec.binary.Hex;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link BlobCodec}.
 */
class BlobCodecTest implements CodecTestSupport<Blob, FieldValue, Class<? super Blob>> {

    private final MockBlob[] blob = {
        new MockBlob(new byte[0]),
        new MockBlob(new byte[]{0}),
        new MockBlob(new byte[]{0x7F}),
        new MockBlob(new byte[]{0x12, 34, 0x56, 78, (byte) 0x9A}),
        new MockBlob("Hello world!".getBytes(StandardCharsets.US_ASCII)),
        new MockBlob(new byte[]{(byte) 0xFE, (byte) 0xDC, (byte) 0xBA}),
        new MockBlob(new byte[0], new byte[0]),
        new MockBlob("Hello, ".getBytes(StandardCharsets.US_ASCII), "R2DBC!".getBytes(StandardCharsets.US_ASCII)),
        new MockBlob(new byte[]{0}, new byte[]{0}, new byte[]{0}),
        new MockBlob(
            new byte[]{(byte) 0xFE, (byte) 0xDC, (byte) 0xBA},
            new byte[]{(byte) 0x98, 0x76, 0x54, 0x32, 0x1},
            new byte[]{0x12, 34, 0x56, 78, (byte) 0x9A}
        ),
        new MockBlob(
            new byte[0],
            new byte[]{(byte) 0x98, 0x76, 0x54, 0x32, 0x1},
            new byte[]{0x12, 34, 0x56, 78, (byte) 0x9A}
        ),
        new MockBlob(
            new byte[]{(byte) 0xFE, (byte) 0xDC, (byte) 0xBA},
            new byte[0],
            new byte[]{0x12, 34, 0x56, 78, (byte) 0x9A}
        ),
        new MockBlob(
            new byte[]{(byte) 0xFE, (byte) 0xDC, (byte) 0xBA},
            new byte[]{(byte) 0x98, 0x76, 0x54, 0x32, 0x1},
            new byte[0]
        ),
    };

    @Override
    public BlobCodec getCodec() {
        return BlobCodec.INSTANCE;
    }

    @Override
    public Blob[] originParameters() {
        return blob;
    }

    @Override
    public Object[] stringifyParameters() {
        String[] results = new String[blob.length];
        for (int i = 0; i < results.length; ++i) {
            String hex = Arrays.stream(blob[i].values)
                .map(it -> it.length == 0 ? "" : Hex.encodeHexString(it, false))
                .collect(Collectors.joining());
            results[i] = String.format("x'%s'", hex);
        }
        return results;
    }

    private static final class MockBlob implements Blob {

        private final byte[][] values;

        private MockBlob(byte[] ...values) {
            this.values = values;
        }

        @Override
        public Flux<ByteBuffer> stream() {
            return Flux.create(sink -> {
                for (byte[] value : values) {
                    ByteBuffer buffer = ByteBuffer.allocate(value.length);
                    buffer.put(value).flip();
                    sink.next(buffer);
                }
                sink.complete();
            });
        }

        @Override
        public Mono<Void> discard() {
            return Mono.empty();
        }
    }
}
