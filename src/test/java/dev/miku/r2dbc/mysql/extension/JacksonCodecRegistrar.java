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

package dev.miku.r2dbc.mysql.extension;

import dev.miku.r2dbc.mysql.codec.CodecRegistry;
import dev.miku.r2dbc.mysql.codec.JacksonCodec;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

/**
 * The JSON {@link CodecRegistrar} based on jackson.
 */
public final class JacksonCodecRegistrar implements CodecRegistrar {

    private static boolean setUp = false;

    @Override
    public void register(ByteBufAllocator allocator, CodecRegistry registry) {
        if (setUp) {
            registry.addFirst(JacksonCodec.DECODING)
                .addLast(JacksonCodec.ENCODING);
        }
    }

    @Override
    public String toString() {
        return "JacksonCodecRegistrar{}";
    }

    public static void setUp() {
        setUp = true;
    }

    public static void tearDown() {
        setUp = false;
    }
}
