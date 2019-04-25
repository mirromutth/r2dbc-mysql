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

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlJson} for Gson.
 */
final class GsonMySqlJson implements MySqlJson {

    private final Gson gson;

    GsonMySqlJson(Gson gson) {
        this.gson = requireNonNull(gson, "gson must not be null");
    }

    @Override
    public void serialize(OutputStream output, Charset charset, @Nullable Object data) throws IOException {
        requireNonNull(output, "output must not be null");
        requireNonNull(charset, "charset must not be null");

        try (Writer writer = new OutputStreamWriter(output, charset)) {
            gson.toJson(data, writer);
        } catch (JsonParseException e) {
            throw new IOException(e);
        }
    }

    @Override
    public <T> T deserialize(InputStream input, Charset charset, Type type) throws IOException {
        requireNonNull(input, "input must not be null");
        requireNonNull(charset, "charset must not be null");
        requireNonNull(type, "type must not be null");

        try (Reader reader = new InputStreamReader(input, charset)) {
            return gson.fromJson(reader, type);
        } catch (JsonParseException e) {
            throw new IOException(e);
        }
    }
}
