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

import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.Charset;

/**
 * A JSON codec based on Jackson.
 */
public final class JacksonCodec implements ParametrizedCodec<Object> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final ByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;

    public static final JacksonCodec ENCODING = new JacksonCodec(false);

    public static final JacksonCodec DECODING = new JacksonCodec(true);

    /**
     * {@code true} if this is decode mode.
     */
    private final boolean decode;

    private JacksonCodec(boolean decode) {
        this.decode = decode;
    }

    @Override
    public Object decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        try (Reader r = new InputStreamReader(new ByteBufInputStream(value), CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset())) {
            return MAPPER.readValue(r, target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object decode(ByteBuf value, FieldInformation info, ParameterizedType target, boolean binary, CodecContext context) {
        try (Reader r = new InputStreamReader(new ByteBufInputStream(value), CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset())) {
            return MAPPER.readValue(r, MAPPER.constructType(target));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new JacksonParameter(ALLOC, value, context);
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

    private static final class JacksonParameter implements Parameter {

        private final ByteBufAllocator allocator;

        private final Object value;

        private final CodecContext context;

        private JacksonParameter(ByteBufAllocator allocator, Object value, CodecContext context) {
            this.allocator = allocator;
            this.value = value;
            this.context = context;
        }

        @Override
        public Flux<ByteBuf> binary() {
            return Flux.create(sink -> {
                Charset charset = context.getClientCollation().getCharset();
                ByteBuf[] buffers = new ByteBuf[2];

                buffers[1] = ALLOC.buffer();

                try (Writer writer = new OutputStreamWriter(new ByteBufOutputStream(buffers[1]), charset)) {
                    MAPPER.writeValue(writer, value);
                } catch (IOException e) {
                    buffers[1].release();
                    sink.error(new RuntimeException(e));
                    return;
                } catch (Throwable e) {
                    buffers[1].release();
                    sink.error(e);
                    return;
                }

                int bytes = buffers[1].readableBytes();
                int varIntSize = VarIntUtils.varIntBytes(bytes);

                try {
                    buffers[0] = ALLOC.buffer(varIntSize, varIntSize);
                    VarIntUtils.writeVarInt(buffers[0], bytes);
                    sink.next(buffers[0]);
                    sink.next(buffers[1]);
                    sink.complete();
                } catch (Throwable e) {
                    ReferenceCountUtil.safeRelease(buffers[0]);
                    buffers[1].release();
                    sink.error(e);
                }
            });
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
            return Mono.fromRunnable(() -> {
                try {
                    MAPPER.writeValue(writer, value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public short getType() {
            return DataTypes.VARCHAR;
        }
    }
}
