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
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.util.List;

/**
 * An interface considers massive data with {@link ParameterizedType} for {@link Codec}.
 *
 * @param <T> the type that is handled by this codec.
 */
public interface MassiveParametrizedCodec<T> extends ParametrizedCodec<T>, MassiveCodec<T> {

    /**
     * Decode a massive value as specified {@link ParameterizedType}.
     *
     * @param value    {@link ByteBuf}s list.
     * @param metadata the metadata of the column.
     * @param target   the specified {@link ParameterizedType}.
     * @param binary   if the value should be decoded by binary protocol.
     * @param context  the codec context.
     * @return the decoded result.
     */
    @Nullable
    Object decodeMassive(List<ByteBuf> value, MySqlColumnMetadata metadata, ParameterizedType target,
        boolean binary,
        CodecContext context);
}
