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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.ParameterOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.platform.commons.function.Try;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A helper of {@link ParamOutputStream} for unit tests.
 * <p>
 * Use reflection to access private methods/fields.
 */
public final class ParameterOutputStreamHelper {

    private static final Constructor<ParamOutputStream> CONSTRUCTOR =
        ReflectionUtils.getDeclaredConstructor(ParamOutputStream.class);

    private static final Field BUFFERS = Try.call(() -> ParamOutputStream.class.getDeclaredField("buffers"))
        .getOrThrow(RuntimeException::new);

    public static ParameterOutputStream get() {
        return ReflectionUtils.newInstance(CONSTRUCTOR, Unpooled.buffer());
    }

    public static ByteBuf getBuffer(ParameterOutputStream stream) {
        @SuppressWarnings("unchecked")
        List<ByteBuf> buffers = (List<ByteBuf>) ReflectionUtils.tryToReadFieldValue(BUFFERS, stream)
            .getOrThrow(RuntimeException::new);
        assertThat(buffers).hasSize(1);
        return buffers.get(0);
    }

    private ParameterOutputStreamHelper() {
    }
}
