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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.r2dbc.spi.Blob;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link BlobCodec}.
 */
class BlobCodecTest implements CodecTestSupport<Blob> {

    private final MockBlob[] blob = {
        new MockBlob(),
        new MockBlob(new byte[0]),
        new MockBlob(new byte[]{0}),
        new MockBlob(new byte[]{0x7F}),
        new MockBlob(new byte[]{0x12, 34, 0x56, 78, (byte) 0x9A}),
        new MockBlob("Hello world!".getBytes(StandardCharsets.US_ASCII)),
        new MockBlob("Hello 日本語（にほんご）!".getBytes()),
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
        new MockBlob(
            StringCodecTest.BIG.getBytes(),
            StringCodecTest.BIG.getBytes(),
            StringCodecTest.BIG.getBytes()
        )
    };

    @Override
    public BlobCodec getCodec(ByteBufAllocator allocator) {
        return new BlobCodec(allocator);
    }

    @Override
    public Blob[] originParameters() {
        return blob;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(blob)
            .map(it -> Arrays.stream(it.values)
                .map(v -> v.length == 0 ? "" : Hex.toHexString(v))
                .collect(Collectors.joining("", "x'", "'")))
            .toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(blob).map(it -> Unpooled.wrappedBuffer(it.values)).toArray(ByteBuf[]::new);
    }

    private static final class MockBlob implements Blob {

        private final byte[][] values;

        private MockBlob(byte[]... values) {
            this.values = values;
        }

        @Override
        public Flux<ByteBuffer> stream() {
            return Flux.create(sink -> {
                for (byte[] value : values) {
                    if (sink.isCancelled()) {
                        return;
                    }

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
