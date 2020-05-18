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

import dev.miku.r2dbc.mysql.message.FieldValue;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Type;

/**
 * Bind all codecs for all types.
 */
public interface Codecs {

    @Nullable
    <T> T decode(FieldValue value, FieldInformation info, Type type, boolean binary, ConnectionContext context);

    <T> T decodeLastInsertId(long value, Class<T> type);

    ParameterValue encode(Object value, ConnectionContext context);

    ParameterValue encodeNull();

    static Codecs getInstance() {
        return DefaultCodecs.getDefault();
    }
}
