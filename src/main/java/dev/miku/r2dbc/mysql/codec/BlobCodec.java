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

import dev.miku.r2dbc.mysql.ParameterOutputStream;
import dev.miku.r2dbc.mysql.ParameterWriter;
import dev.miku.r2dbc.mysql.codec.lob.LobUtils;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.Parameter;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Blob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Codec for {@link Blob}.
 * <p>
 * Note: {@link Blob} will be written by {@code ParameterOutputStream} rather than {@link #encode}.
 */
final class BlobCodec implements MassiveCodec<Blob> {

    static final BlobCodec INSTANCE = new BlobCodec();

    private BlobCodec() {
    }

    @Override
    public Blob decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
        return LobUtils.createBlob(value);
    }

    @Override
    public Blob decodeMassive(List<ByteBuf> value, FieldInformation info, Class<?> target, boolean binary, CodecContext context) {
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
        return new BlobParameter((Blob) value);
    }

    private static final class BlobParameter extends AbstractLobParameter {

        private final AtomicReference<Blob> blob;

        private BlobParameter(Blob blob) {
            this.blob = new AtomicReference<>(blob);
        }

        @Override
        public Mono<Void> binary(ParameterOutputStream output) {
            return Mono.defer(() -> {
                Blob blob = this.blob.getAndSet(null);

                if (blob == null) {
                    return Mono.error(new IllegalStateException("Blob has written, can not write twice"));
                }

                // Need count entire length, so can not streaming here.
                // Must have defaultIfEmpty, try Mono.fromCallable(() -> null).flux().collectList()
                return Flux.from(blob.stream())
                    .collectList()
                    .defaultIfEmpty(Collections.emptyList())
                    .doOnNext(output::writeByteBuffers)
                    .then();
            });
        }

        @Override
        public Mono<Void> text(ParameterWriter writer) {
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
