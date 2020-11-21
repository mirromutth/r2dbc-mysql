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

package dev.miku.r2dbc.mysql.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.miku.r2dbc.mysql.Parameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.codec.CodecContext;
import dev.miku.r2dbc.mysql.codec.FieldInformation;
import dev.miku.r2dbc.mysql.codec.ParametrizedCodec;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
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

    private final ByteBufAllocator allocator;

    private final Mode mode;

    public JacksonCodec(ByteBufAllocator allocator, Mode mode) {
        this.allocator = allocator;
        this.mode = mode;
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
        return new JacksonParameter(allocator, value, context);
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
        return mode.isEncode();
    }

    private boolean doCanDecode(FieldInformation info) {
        return mode.isDecode() && info.getType() == DataTypes.JSON && info.getCollationId() != CharCollation.BINARY_ID;
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
        public Mono<ByteBuf> publishBinary() {
            return Mono.fromSupplier(() -> {
                int reserved;
                Charset charset = context.getClientCollation().getCharset();
                ByteBuf content = allocator.buffer();

                try (Writer w = new OutputStreamWriter(new ByteBufOutputStream(content), charset)) {
                    VarIntUtils.reserveVarInt(content);
                    reserved = content.readableBytes();
                    MAPPER.writeValue(w, value);
                } catch (IOException e) {
                    content.release();
                    throw new RuntimeException(e);
                } catch (Throwable e) {
                    content.release();
                    throw e;
                }

                try {
                    return VarIntUtils.setReservedVarInt(content, content.readableBytes() - reserved);
                } catch (Throwable e) {
                    content.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
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

    public enum Mode {

        ALL {

            @Override
            boolean isEncode() {
                return true;
            }

            @Override
            boolean isDecode() {
                return true;
            }
        },
        ENCODE {

            @Override
            boolean isEncode() {
                return true;
            }

            @Override
            boolean isDecode() {
                return false;
            }
        },
        DECODE {

            @Override
            boolean isEncode() {
                return false;
            }

            @Override
            boolean isDecode() {
                return true;
            }
        };

        abstract boolean isEncode();

        abstract boolean isDecode();
    }
}
