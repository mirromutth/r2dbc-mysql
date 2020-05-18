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

import dev.miku.r2dbc.mysql.codec.lob.LobUtils;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import dev.miku.r2dbc.mysql.util.CodecUtils;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Blob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Codec for {@link Blob}.
 * <p>
 * Note: {@link Blob} will be written by {@code ParameterWriter} rather than {@link #encode}.
 */
final class BlobCodec implements Codec<Blob> {

    static final BlobCodec INSTANCE = new BlobCodec();

    private BlobCodec() {
    }

    @Override
    public Blob decode(ByteBuf value, FieldInformation info, Type target, boolean binary, ConnectionContext context) {
        return LobUtils.createBlob(value);
    }

    @Override
    public Blob decodeMassive(List<ByteBuf> value, FieldInformation info, Type target, boolean binary, ConnectionContext context) {
        return LobUtils.createBlob(value);
    }

    @Override
    public boolean canDecode(boolean massive, FieldInformation info, Type target) {
        if (!(target instanceof Class<?>)) {
            return false;
        }

        short type = info.getType();
        if (!TypePredicates.isLob(type) && DataTypes.GEOMETRY != type) {
            return false;
        }

        return ((Class<?>) target).isAssignableFrom(Blob.class);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Blob;
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new BlobValue((Blob) value);
    }

    private static final class BlobValue extends AbstractLobValue {

        private final AtomicReference<Blob> blob;

        private BlobValue(Blob blob) {
            this.blob = new AtomicReference<>(blob);
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.defer(() -> {
                Blob blob = this.blob.getAndSet(null);

                if (blob == null) {
                    return Mono.error(new IllegalStateException("Blob has written, can not write twice"));
                }

                // Need count entire length, so can not streaming here.
                return Flux.from(blob.stream())
                    .collectList()
                    .doOnNext(writer::writeByteBuffers)
                    .then();
            });
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.defer(() -> {
                Blob blob = this.blob.getAndSet(null);

                if (blob == null) {
                    return Mono.error(new IllegalStateException("Blob has written, can not write twice"));
                }

                return Flux.from(blob.stream())
                    .doOnSubscribe(ignored -> builder.append('x').append('\''))
                    .doOnNext(it -> CodecUtils.appendHex(builder, it))
                    .doOnComplete(() -> builder.append('\''))
                    .then();
            });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BlobValue)) {
                return false;
            }

            BlobValue blobValue = (BlobValue) o;

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
