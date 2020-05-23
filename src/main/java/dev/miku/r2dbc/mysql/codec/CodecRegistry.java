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

/**
 * Registry allowing to query and register {@link Codec}s.
 */
public interface CodecRegistry {

    /**
     * Register codec before all other codecs.
     *
     * @param codec the codec to register
     * @throws IllegalArgumentException if {@code codec} is {@code null}
     * @return this {@link CodecRegistry}
     */
    CodecRegistry addFirst(Codec<?> codec);

    /**
     * Register codec after all other codecs.
     *
     * @param codec the codec to register
     * @throws IllegalArgumentException if {@code codec} is {@code null}
     * @return this {@link CodecRegistry}
     */
    CodecRegistry addLast(Codec<?> codec);
}
