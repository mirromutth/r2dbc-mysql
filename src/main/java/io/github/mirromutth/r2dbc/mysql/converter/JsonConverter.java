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

package io.github.mirromutth.r2dbc.mysql.converter;

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.ColumnType;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.json.MySqlJson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Converter for JSON.
 */
final class JsonConverter implements Converter<Object, Type> {

    private final MySqlJson mySqlJson;

    JsonConverter(MySqlJson mySqlJson) {
        this.mySqlJson = requireNonNull(mySqlJson, "mySqlJson must not be null");
    }

    @Override
    public Object read(ByteBuf buf, boolean isUnsigned, int precision, int collationId, Type target, MySqlSession session) {
        Charset charset = CharCollation.fromId(collationId, session.getServerVersion()).getCharset();

        try (ByteBufInputStream input = new ByteBufInputStream(buf)) {
            return mySqlJson.deserialize(input, charset, target);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean canRead(ColumnType type, boolean isUnsigned, int precision, int collationId, Type target, MySqlSession session) {
        // any java type if column type is json.
        return ColumnType.JSON == type;
    }
}
