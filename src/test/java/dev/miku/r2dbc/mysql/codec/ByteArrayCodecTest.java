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

import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link ByteArrayCodec}.
 */
class ByteArrayCodecTest implements CodecTestSupport<byte[], NormalFieldValue, Class<? super byte[]>> {

    private final byte[][] bytes = {
        new byte[0],
        new byte[]{0},
        new byte[]{0x7F},
        new byte[]{0x12, 34, 0x56, 78, (byte) 0x9A},
        "Hello world!".getBytes(StandardCharsets.US_ASCII),
        new byte[]{(byte) 0xFE, (byte) 0xDC, (byte) 0xBA},
    };

    @Override
    public ByteArrayCodec getCodec() {
        return ByteArrayCodec.INSTANCE;
    }

    @Override
    public byte[][] originParameters() {
        return bytes;
    }

    @Override
    public Object[] stringifyParameters() {
        String[] results = new String[bytes.length];
        for (int i = 0; i < results.length; ++i) {
            results[i] = String.format("x'%s'", Hex.encodeHexString(bytes[i], false));
        }
        return results;
    }
}
