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
import dev.miku.r2dbc.mysql.codec.lob.LobUtils;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Blob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Codec for {@link Blob}.
 */
final class BlobCodec implements MassiveCodec<Blob> {

    private static final int MAX_MERGE = 1 << 14;

    private final ByteBufAllocator allocator;

    BlobCodec(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public Blob decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary,
        CodecContext context) {
        return LobUtils.createBlob(value);
    }

    @Override
    public Blob decodeMassive(List<ByteBuf> value, FieldInformation info, Class<?> target, boolean binary,
        CodecContext context) {
        return LobUtils.createBlob(value);
    }

    @Override
    public boolean canDecode(FieldInformation info, Class<?> target) {
        short type = info.getType();
        if (!TypePredicates.isLob(type) && DataTypes.GEOMETRY != type) {
            return false;
        }

        return target.isAssignableFrom(Blob.class);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Blob;
    }

    @Override
    public Parameter encode(Object value, CodecContext context) {
        return new BlobParameter(allocator, (Blob) value);
    }

    static List<ByteBuf> toList(List<ByteBuf> buffers) {
        switch (buffers.size()) {
            case 0:
                return Collections.emptyList();
            case 1:
                return Collections.singletonList(buffers.get(0));
            default:
                return buffers;
        }
    }

    static void releaseAll(List<ByteBuf> buffers, ByteBuf lastBuf) {
        boolean nonLast = true;

        for (ByteBuf buf : buffers) {
            ReferenceCountUtil.safeRelease(buf);
            if (buf == lastBuf) {
                nonLast = false;
            }
        }

        if (nonLast) {
            lastBuf.release();
        }
    }

    private static final class BlobParameter extends AbstractLobParameter {

        private final ByteBufAllocator allocator;

        private final AtomicReference<Blob> blob;

        private BlobParameter(ByteBufAllocator allocator, Blob blob) {
            this.allocator = allocator;
            this.blob = new AtomicReference<>(blob);
        }

        @Override
        public Flux<ByteBuf> publishBinary() {
            return Flux.defer(() -> {
                Blob blob = this.blob.getAndSet(null);

                if (blob == null) {
                    return Flux.error(new IllegalStateException("Blob has written, can not write twice"));
                }

                // Must have defaultIfEmpty, try Mono.fromCallable(() -> null).flux().collectList()
                return Flux.from(blob.stream())
                    .collectList()
                    .defaultIfEmpty(Collections.emptyList())
                    .flatMapIterable(list -> {
                        if (list.isEmpty()) {
                            // It is zero of var int, not terminal.
                            return Collections.singletonList(allocator.buffer(Byte.BYTES).writeByte(0));
                        }

                        long bytes = 0;
                        List<ByteBuf> buffers = new ArrayList<>();
                        ByteBuf lastBuf = allocator.buffer();

                        try {
                            ByteBuf firstBuf = lastBuf;

                            buffers.add(firstBuf);
                            VarIntUtils.reserveVarInt(firstBuf);

                            for (ByteBuffer src : list) {
                                if (src.hasRemaining()) {
                                    int size = src.remaining();
                                    bytes += size;

                                    // size + lastBuf.readableBytes() > MAX_MERGE
                                    if (size > MAX_MERGE - lastBuf.readableBytes()) {
                                        lastBuf = allocator.buffer();
                                        buffers.add(lastBuf);
                                    }

                                    lastBuf.writeBytes(src);
                                }
                            }

                            VarIntUtils.setReservedVarInt(firstBuf, bytes);

                            return toList(buffers);
                        } catch (Throwable e) {
                            releaseAll(buffers, lastBuf);
                            throw e;
                        }
                    });
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.defer(() -> {
                Blob blob = this.blob.getAndSet(null);

                if (blob == null) {
                    return Mono.error(new IllegalStateException("Blob has written, can not write twice"));
                }

                return Flux.from(blob.stream())
                    .doOnSubscribe(ignored -> writer.startHex())
                    .doOnNext(writer::writeHex)
                    .then();
            });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BlobParameter)) {
                return false;
            }

            BlobParameter blobValue = (BlobParameter) o;

            return Objects.equals(this.blob.get(), blobValue.blob.get());
        }

        @Override
        public int hashCode() {
            Blob blob = this.blob.get();
            return blob == null ? 0 : blob.hashCode();
        }

        @Override
        protected Publisher<Void> getDiscard() {
            Blob blob = this.blob.getAndSet(null);
            return blob == null ? null : blob.discard();
        }
    }
}
