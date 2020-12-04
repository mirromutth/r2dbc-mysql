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

import dev.miku.r2dbc.mysql.Parameter;
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

    @Nullable
    T decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context);

    boolean canDecode(FieldInformation info, Class<?> target);

    boolean canEncode(Object value);

    Parameter encode(Object value, CodecContext context);
}
