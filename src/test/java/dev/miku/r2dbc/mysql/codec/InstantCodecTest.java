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

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * Unit tests for {@link InstantCodec}.
 */
class InstantCodecTest extends DateTimeCodecTestSupport<Instant> {

    private final Instant[] instants = {
        Instant.EPOCH,
        Instant.ofEpochMilli(-1000),
        Instant.ofEpochMilli(1000),
        Instant.ofEpochMilli(-1577777771671L),
        Instant.ofEpochMilli(1577777771671L),
        Instant.ofEpochSecond(-30557014167219200L),
        Instant.ofEpochSecond(30557014167219200L),
    };

    @Override
    public Codec<Instant> getCodec(ByteBufAllocator allocator) {
        return new InstantCodec(allocator);
    }

    @Override
    public Instant[] originParameters() {
        return instants;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(instants).map(this::convert).map(this::toText).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(instants).map(this::convert).map(this::toBinary).toArray(ByteBuf[]::new);
    }

    private LocalDateTime convert(Instant value) {
        return LocalDateTime.ofInstant(value, SERVER_ZONE_ID);
    }
}
