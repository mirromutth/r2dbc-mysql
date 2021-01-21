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

/**
 * The builder for {@link Codecs}.
 * <p>
 * Note: user should not remove/change any built-in {@link Codec} because it will cause the driver to not
 * work, so now this interface has only functions of "add".
 */
public interface CodecsBuilder extends CodecRegistry {

    /**
     * {@inheritDoc}
     */
    @Override
    CodecsBuilder addFirst(Codec<?> codec);

    /**
     * {@inheritDoc}
     */
    @Override
    CodecsBuilder addLast(Codec<?> codec);

    /**
     * Build a {@link Codecs} from current codecs.
     *
     * @return a {@link Codecs}.
     */
    Codecs build();
}
