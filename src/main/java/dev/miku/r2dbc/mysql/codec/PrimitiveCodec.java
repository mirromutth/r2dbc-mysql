/*
 * Copyright 2018-2021 the original author or authors.
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
import reactor.util.annotation.NonNull;

/**
 * Base class considers primitive class for {@link Codec} implementations. It should be an internal
 * abstraction.
 * <p>
 * Primitive types should never return {@code null} when decoding.
 *
 * @param <T> the boxed type that is handled by this codec.
 */
interface PrimitiveCodec<T> extends Codec<T> {

    /**
     * Decode a {@link ByteBuf} as specified {@link Class}.
     *
     * @param value   the {@link ByteBuf}.
     * @param info    the information of this value.
     * @param target  the specified {@link Class}, which can be a primitive type.
     * @param binary  if the value should be decoded by binary protocol.
     * @param context the codec context.
     * @return the decoded data that is boxed.
     */
    @NonNull
    @Override
    T decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context);

    /**
     * Check if can decode the field value as a primitive data.
     *
     * @param info the information of this value.
     * @return if can decode.
     */
    boolean canPrimitiveDecode(FieldInformation info);

    /**
     * Get the primitive {@link Class}, such as {@link Integer#TYPE}, etc.
     *
     * @return the primitive {@link Class}.
     */
    Class<T> getPrimitiveClass();
}
