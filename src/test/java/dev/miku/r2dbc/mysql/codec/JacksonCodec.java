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

import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.Parameter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.ParameterizedType;

/**
 * A JSON codec based on Jackson.
 */
public final class JacksonCodec implements ParametrizedCodec<Object> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final JacksonCodec ENCODING = new JacksonCodec(MAPPER, false);

    public static final JacksonCodec DECODING = new JacksonCodec(MAPPER, true);

    private final ObjectMapper mapper;

    /**
     * {@code true} if this is decode mode.
     */
    private final boolean decode;

    private JacksonCodec(ObjectMapper mapper, boolean decode) {
        this.mapper = mapper;
        this.decode = decode;
    }

    @Override
    public Object decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        try (Reader r = new InputStreamReader(new ByteBufInputStream(value), CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset())) {
            return mapper.readValue(r, target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object decode(ByteBuf value, FieldInformation info, ParameterizedType target, boolean binary, CodecContext context) {
        try (Reader r = new InputStreamReader(new ByteBufInputStream(value), CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset())) {
            return mapper.readValue(r, mapper.constructType(target));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        // TODO: refactor parameter values and implement this.
        throw new IllegalStateException("TODO");
    }

    @Override
    public boolean canDecode(FieldInformation info, Class<?> target) {
        return doCanDecode(info);
    }

    @Override
    public boolean canDecode(FieldInformation info, ParameterizedType target) {
        return doCanDecode(info);
    }

    @Override
    public boolean canEncode(Object value) {
        return !decode;
    }

    private boolean doCanDecode(FieldInformation info) {
        return decode && info.getType() == DataTypes.JSON && info.getCollationId() != CharCollation.BINARY_ID;
    }
}
