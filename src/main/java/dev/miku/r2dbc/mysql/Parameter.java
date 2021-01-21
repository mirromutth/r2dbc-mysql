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

package dev.miku.r2dbc.mysql;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * A parameter value bound to an {@link Binding}.
 * <p>
 * TODO: add ScalarParameter for better performance.
 */
public interface Parameter extends Disposable {

    /**
     * Note: the {@code null} is processed by built-in codecs.
     *
     * @return {@code true} if it is {@code null}. Codec extensions should always return {@code false}.
     */
    default boolean isNull() {
        return false;
    }

    /**
     * Binary protocol encoding. See MySQL protocol documentations, if don't want to support the binary
     * protocol, please receive an exception.
     * <p>
     * Note: not like the text protocol, it make a sense for state-less.
     * <p>
     * Binary protocol maybe need to add a var-integer length before encoded content. So if makes it like
     * {@code Mono<Void> publishBinary (Xxx binaryWriter)}, and if supports multiple times writing like a
     * {@code OutputStream} or {@code Writer} for each parameter, this make a hell of a complex state system.
     * If we don't support multiple times writing, it will be hard to understand and maybe make a confuse to
     * user.
     *
     * @return the encoded binary buffer(s).
     */
    Publisher<ByteBuf> publishBinary();

    /**
     * Text protocol encoding.
     * <p>
     * Note: not like the binary protocol, it make a sense for copy-less.
     * <p>
     * If it seems like {@code Publisher<? extends CharSequence> publishText()}, then we need to always deep
     * copy results (with escaping) into the string buffer of the synthesized SQL statement.
     * <p>
     * WARNING: the {@code output} requires state synchronization after this function called, so if external
     * writer buffered the {@code writer}, please flush the external buffer before receiving the completion
     * signal.
     *
     * @param writer the text protocol writer, extended {@code Writer}, not thread-safety.
     * @return the encoding completion signal.
     */
    Mono<Void> publishText(ParameterWriter writer);

    /**
     * If don't want to support the binary protocol, please throw an exception.
     *
     * @return the MySQL data type of this parameter data, see {@code DataTypes}.
     */
    short getType();

    /**
     * {@inheritDoc}
     */
    @Override
    default void dispose() { }
}
