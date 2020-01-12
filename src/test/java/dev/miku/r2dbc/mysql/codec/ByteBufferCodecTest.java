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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import org.testcontainers.shaded.org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link ByteBufferCodec}.
 */
class ByteBufferCodecTest implements CodecTestSupport<ByteBuffer, NormalFieldValue, Class<? super ByteBuffer>> {

    private final ByteBuffer[] buffers = {
        ByteBuffer.allocate(0),
        ByteBuffer.allocate(1),
        ByteBuffer.wrap(new byte[]{0x7F}),
        ByteBuffer.wrap(new byte[]{0x12, 34, 0x56, 78, (byte) 0x9A}),
        ByteBuffer.wrap("Hello world!".getBytes(StandardCharsets.US_ASCII)),
        ByteBuffer.wrap(new byte[]{(byte) 0xFE, (byte) 0xDC, (byte) 0xBA}),
    };

    @Override
    public ByteBufferCodec getCodec() {
        return ByteBufferCodec.INSTANCE;
    }

    @Override
    public ByteBuffer[] originParameters() {
        ByteBuffer[] results = new ByteBuffer[buffers.length];
        for (int i = 0; i < results.length; ++i) {
            results[i] = buffers[i].slice();
        }
        return results;
    }

    @Override
    public Object[] stringifyParameters() {
        String[] results = new String[buffers.length];
        for (int i = 0; i < buffers.length; ++i) {
            results[i] = String.format("x'%s'", Hex.encodeHexString(buffers[i], false));
        }
        return results;
    }
}
