/*
 * Copyright 2018-2019 the original author or authors.
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

package io.github.mirromutth.r2dbc.mysql.json;

import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

/**
 * JSON serializer for MySQL type json.
 *
 * WARNING: should make sure implementations are thread-safety.
 */
public interface MySqlJson {

    /**
     * WARNING: should close {@code output} if {@code output} and {@code charset} not be null.
     *
     * @param output output stream that want to write.
     * @param charset JSON string charset.
     * @param data that want write to {@code out} serialized by JSON.
     * @throws IllegalArgumentException {@code output} or {@code charset} is null.
     * @throws IOException {@code data} can not be serialized by the specified JSON serializer.
     */
    void serialize(OutputStream output, Charset charset, @Nullable Object data) throws IllegalArgumentException, IOException;

    /**
     * WARNING: should close {@code output} if {@code output}, {@code charset} and {@code type} not be null.
     *
     * @param input
     * @param charset
     * @param type
     * @param <T>
     * @return
     * @throws IllegalArgumentException {@code output}, {@code charset} or {@code type} is null.
     * @throws IOException data of {@code reader} can not deserialize to {@code type}.
     */
    <T> T deserialize(InputStream input, Charset charset, Type type) throws IllegalArgumentException, IOException;
}
