/*
 * Copyright 2018-2021 the original author or authors.
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

import dev.miku.r2dbc.mysql.MySqlColumnMetadata;
import dev.miku.r2dbc.mysql.MySqlParameter;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.codec.lob.LobUtils;
import dev.miku.r2dbc.mysql.constant.MySqlType;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.Clob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Codec for {@link Clob}.
 */
final class ClobCodec implements MassiveCodec<Clob> {

    /**
     * It should less than {@link BlobCodec}'s, because we can only make a minimum estimate before writing the
     * string.
     */
    private static final int MAX_MERGE = 1 << 13;

    private final ByteBufAllocator allocator;

    ClobCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public Clob decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        return LobUtils.createClob(value, metadata.getCharCollation(context));
    }

    @Override
    public Clob decodeMassive(List<ByteBuf> value, MySqlColumnMetadata metadata, Class<?> target,
        boolean binary, CodecContext context) {
        return LobUtils.createClob(value, metadata.getCharCollation(context));
    }

    @Override
    public boolean canDecode(MySqlColumnMetadata metadata, Class<?> target) {
        MySqlType type = metadata.getType();

        return (type.isLob() || type == MySqlType.JSON) && target.isAssignableFrom(Clob.class);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Clob;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new ClobMySqlParameter(allocator, (Clob) value, context);
    }

    private static class ClobMySqlParameter extends AbstractLobMySqlParameter {

        private final ByteBufAllocator allocator;

        private final AtomicReference<Clob> clob;

        private final CodecContext context;

        private ClobMySqlParameter(ByteBufAllocator allocator, Clob clob, CodecContext context) {
            this.allocator = allocator;
            this.clob = new AtomicReference<>(clob);
            this.context = context;
        }

        @Override
        public Flux<ByteBuf> publishBinary() {
            return Flux.defer(() -> {
                Clob clob = this.clob.getAndSet(null);

                if (clob == null) {
                    return Mono.error(new IllegalStateException("Clob has written, can not write twice"));
                }

                // Must have defaultIfEmpty, try Mono.fromCallable(() -> null).flux().collectList()
                return Flux.from(clob.stream())
                    .collectList()
                    .defaultIfEmpty(Collections.emptyList())
                    .flatMapIterable(list -> {
                        if (list.isEmpty()) {
                            // It is zero of var int, not terminal.
                            return Collections.singletonList(allocator.buffer(Byte.BYTES).writeByte(0));
                        }

                        long bytes = 0;
                        Charset charset = context.getClientCollation().getCharset();
                        List<ByteBuf> buffers = new ArrayList<>();
                        ByteBuf lastBuf = allocator.buffer();

                        try {
                            ByteBuf firstBuf = lastBuf;

                            buffers.add(firstBuf);
                            VarIntUtils.reserveVarInt(firstBuf);

                            for (CharSequence src : list) {
                                int length = src.length();

                                if (length > 0) {
                                    // size + lastBuf.readableBytes() > MAX_MERGE, it just a minimum estimate.
                                    if (length > MAX_MERGE - lastBuf.readableBytes()) {
                                        lastBuf = allocator.buffer();
                                        buffers.add(lastBuf);
                                    }

                                    bytes += lastBuf.writeCharSequence(src, charset);
                                }
                            }

                            VarIntUtils.setReservedVarInt(firstBuf, bytes);

                            return BlobCodec.toList(buffers);
                        } catch (Throwable e) {
                            BlobCodec.releaseAll(buffers, lastBuf);
                            throw e;
                        }
                    });
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.defer(() -> {
                Clob clob = this.clob.getAndSet(null);

                if (clob == null) {
                    return Mono.error(new IllegalStateException("Clob has written, can not write twice"));
                }

                return Flux.from(clob.stream())
                    .doOnSubscribe(ignored -> writer.startString())
                    .doOnNext(writer::append)
                    .then();
            });
        }

        @Override
        public MySqlType getType() {
            return MySqlType.LONGTEXT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ClobMySqlParameter)) {
                return false;
            }

            ClobMySqlParameter clobValue = (ClobMySqlParameter) o;

            return Objects.equals(this.clob.get(), clobValue.clob.get());
        }

        @Override
        public int hashCode() {
            Clob clob = this.clob.get();
            return clob == null ? 0 : clob.hashCode();
        }

        @Override
        protected Publisher<Void> getDiscard() {
            Clob clob = this.clob.getAndSet(null);
            return clob == null ? null : clob.discard();
        }
    }
}
