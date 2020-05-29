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

import java.lang.reflect.ParameterizedType;
import java.util.List;

/**
 * An interface considers massive data with {@link ParameterizedType} for {@link Codec}.
 */
public interface MassiveParametrizedCodec<T> extends ParametrizedCodec<T>, MassiveCodec<T> {

    @Nullable
    Object decodeMassive(List<ByteBuf> value, FieldInformation info, ParameterizedType target, boolean binary, CodecContext context);
}
