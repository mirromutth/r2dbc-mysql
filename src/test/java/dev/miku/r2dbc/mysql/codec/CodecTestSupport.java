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

import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.ParameterOutputStream;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import dev.miku.r2dbc.mysql.message.client.ParameterOutputStreamHelper;
import dev.miku.r2dbc.mysql.message.client.ParameterWriterHelper;
import dev.miku.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.escape.Escaper;
import org.testcontainers.shaded.com.google.common.escape.Escapers;
import reactor.test.StepVerifier;

import java.nio.charset.Charset;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class considers unit tests for implementations of {@link Codec}.
 */
interface CodecTestSupport<T> {

    ConnectionContext CONTEXT = new ConnectionContext(ZeroDateOption.USE_NULL);

    Escaper ESCAPER = Escapers.builder()
        .addEscape('\'', "''")
        .addEscape('\0', "\\0")
        .addEscape('\r', "\\r")
        .addEscape('\n', "\\n")
        .addEscape('\\', "\\\\")
        .addEscape('\032', "\\Z")
        .build();

    @Test
    default void binary() {
        Codec<T> codec = getCodec();
        T[] origin = originParameters();
        ByteBuf[] binaries = binaryParameters(CharCollation.clientCharCollation().getCharset());

        assertEquals(origin.length, binaries.length);

        for (int i = 0; i < origin.length; ++i) {
            ParameterOutputStream stream = ParameterOutputStreamHelper.get();
            codec.encode(origin[i], CONTEXT)
                .binary(stream)
                .as(StepVerifier::create)
                .verifyComplete();
            ByteBuf buf = ParameterOutputStreamHelper.getBuffer(stream);
            ByteBuf ans = sized(binaries[i]);

            try {
                assertEquals(buf, ans);
            } finally {
                buf.release();
                ans.release();
            }
        }
    }

    @Test
    default void stringify() {
        Codec<T> codec = getCodec();
        T[] origin = originParameters();
        Object[] strings = stringifyParameters();

        assertEquals(origin.length, strings.length);

        for (int i = 0; i < origin.length; ++i) {
            ParameterWriter writer = ParameterWriterHelper.get(1);
            codec.encode(origin[i], CONTEXT)
                .text(writer)
                .as(StepVerifier::create)
                .verifyComplete();
            assertEquals(ParameterWriterHelper.toSql(writer), strings[i].toString());
        }
    }

    /**
     * If encoding no need sized, override it and just return origin value.
     */
    default ByteBuf sized(ByteBuf value) {
        ByteBuf varInt = Unpooled.buffer();
        CodecUtils.writeVarInt(varInt, value.readableBytes());
        return Unpooled.wrappedBuffer(varInt, value);
    }

    Codec<T> getCodec();

    T[] originParameters();

    Object[] stringifyParameters();

    ByteBuf[] binaryParameters(Charset charset);
}
