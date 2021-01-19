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
import dev.miku.r2dbc.mysql.Parameter;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;

/**
 * Bind all codecs for all types.
 */
public interface Codecs {

    /**
     * Decode a {@link FieldValue} as specified {@link Class type}.
     *
     * @param value   the {@link FieldValue}.
     * @param info    the information of this value.
     * @param type    the specified {@link Class}.
     * @param binary  if the value should be decoded by binary protocol.
     * @param context the codec context.
     * @param <T>     the generic result type.
     * @return the decoded result.
     * @throws IllegalArgumentException if any parameter is {@code null}, or {@code value} cannot be decoded.
     */
    @Nullable
    <T> T decode(FieldValue value, FieldInformation info, Class<?> type, boolean binary,
        CodecContext context);

    /**
     * Decode a {@link FieldValue} as a specified {@link ParameterizedType type}.
     *
     * @param value   the {@link FieldValue}.
     * @param info    the information of this value.
     * @param type    the specified {@link ParameterizedType}.
     * @param binary  if the value should be decoded by binary protocol.
     * @param context the codec context.
     * @param <T>     the generic result type.
     * @return the decoded result.
     * @throws IllegalArgumentException if any parameter is {@code null}, or {@code value} cannot be decoded.
     */
    @Nullable
    <T> T decode(FieldValue value, FieldInformation info, ParameterizedType type, boolean binary,
        CodecContext context);

    /**
     * Decode the last inserted ID from {@code OkMessage} as a specified {@link ParameterizedType type}.
     *
     * @param value the last inserted ID.
     * @param type  the specified {@link Class}.
     * @param <T>   the generic result type.
     * @return the decoded result.
     * @throws IllegalArgumentException if {@code type} is {@code null}, or cannot decode a last inserted ID
     *                                  as {@code type}.
     */
    <T> T decodeLastInsertId(long value, Class<T> type);

    /**
     * Encode a value to a {@link Parameter}.
     *
     * @param value   the value which should be decoded.
     * @param context the codec context.
     * @return encoded {@link Parameter}.
     * @throws IllegalArgumentException if any parameter is {@code null}, or {@code value} cannot be encoded.
     */
    Parameter encode(Object value, CodecContext context);

    /**
     * Encode a null {@link Parameter}.
     *
     * @return a {@link Parameter} take a {@code null} value.
     */
    Parameter encodeNull();

    /**
     * Create a builder from a {@link ByteBufAllocator}.
     *
     * @param allocator the {@link ByteBufAllocator}.
     * @return a {@link CodecsBuilder}.
     */
    static CodecsBuilder builder(ByteBufAllocator allocator) {
        return new DefaultCodecs.Builder(allocator);
    }
}
