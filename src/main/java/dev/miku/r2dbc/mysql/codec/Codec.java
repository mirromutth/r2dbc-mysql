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

import dev.miku.r2dbc.mysql.MySqlColumnMetadata;
import dev.miku.r2dbc.mysql.MySqlParameter;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

/**
 * Codec to encode and decode values based on MySQL data binary/text protocol.
 * <p>
 * Use {@link ParametrizedCodec} for support {@code ParameterizedType} encoding/decoding.
 *
 * @param <T> the type that is handled by this codec.
 */
public interface Codec<T> {

    /**
     * Decode a {@link ByteBuf} as specified {@link Class}.
     *
     * @param value    the {@link ByteBuf}.
     * @param metadata the metadata of the column.
     * @param target   the specified {@link Class}.
     * @param binary   if the value should be decoded by binary protocol.
     * @param context  the codec context.
     * @return the decoded result.
     */
    @Nullable
    T decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context);

    /**
     * Check if can decode the field value as specified {@link Class}.
     *
     * @param metadata the metadata of the column.
     * @param target   the specified {@link Class}.
     * @return if can decode.
     */
    boolean canDecode(MySqlColumnMetadata metadata, Class<?> target);

    /**
     * Check if can encode the specified value.
     *
     * @param value the specified value.
     * @return if can encode.
     */
    boolean canEncode(Object value);

    /**
     * Encode a value to a {@link MySqlParameter}.
     *
     * @param value   the specified value.
     * @param context the codec context.
     * @return encoded {@link MySqlParameter}.
     */
    MySqlParameter encode(Object value, CodecContext context);
}
