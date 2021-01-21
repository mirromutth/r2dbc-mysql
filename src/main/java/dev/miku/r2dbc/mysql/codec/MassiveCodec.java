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
import reactor.util.annotation.Nullable;

import java.util.List;

/**
 * An interface considers massive data for {@link Codec}.
 *
 * @param <T> the type that is handled by this codec.
 */
public interface MassiveCodec<T> extends Codec<T> {

    /**
     * Decode a massive value as specified {@link Class}.
     *
     * @param value   {@link ByteBuf}s list.
     * @param info    the information of this value.
     * @param target  the specified {@link Class}.
     * @param binary  if the value should be decoded by binary protocol.
     * @param context the codec context.
     * @return the decoded result.
     */
    @Nullable
    T decodeMassive(List<ByteBuf> value, FieldInformation info, Class<?> target, boolean binary,
        CodecContext context);
}
