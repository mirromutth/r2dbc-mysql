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
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import dev.miku.r2dbc.mysql.util.CodecUtils;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Clob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Codec for {@link Clob}.
 * <p>
 * Note: {@link Clob} will be written by {@code ParameterWriter} rather than {@link #encode}.
 */
final class ClobCodec implements Codec<Clob> {

    static final ClobCodec INSTANCE = new ClobCodec();

    private ClobCodec() {
    }

    @Override
    public Clob decode(ByteBuf value, FieldInformation info, Class<?> target, boolean binary, ConnectionContext context) {
        return LobUtils.createClob(value, info.getCollationId(), context.getServerVersion());
    }

    @Override
    public Clob decodeMassive(List<ByteBuf> value, FieldInformation info, Class<?> target, boolean binary, ConnectionContext context) {
        return LobUtils.createClob(value, info.getCollationId(), context.getServerVersion());
    }

    @Override
    public boolean canDecode(boolean massive, FieldInformation info, Class<?> target) {
        if (info.getCollationId() == CharCollation.BINARY_ID) {
            return false;
        }

        short type = info.getType();
        if (!TypePredicates.isLob(type) && DataTypes.JSON != type) {
            return false;
        }

        return target.isAssignableFrom(Clob.class);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Clob;
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new ClobValue((Clob) value, context);
    }

    private static class ClobValue extends AbstractLobValue {

        private final AtomicReference<Clob> clob;

        private final ConnectionContext context;

        private ClobValue(Clob clob, ConnectionContext context) {
            this.clob = new AtomicReference<>(clob);
            this.context = context;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.defer(() -> {
                Clob clob = this.clob.getAndSet(null);

                if (clob == null) {
                    return Mono.error(new IllegalStateException("Clob has written, can not write twice"));
                }

                return Flux.from(clob.stream())
                    .collectList()
                    .doOnNext(sequences -> writer.writeCharSequences(sequences, context.getCollation()))
                    .then();
            });
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.defer(() -> {
                Clob clob = this.clob.getAndSet(null);

                if (clob == null) {
                    return Mono.error(new IllegalStateException("Clob has written, can not write twice"));
                }

                return Flux.from(clob.stream())
                    .doOnSubscribe(ignored -> builder.append('\''))
                    .doOnNext(it -> CodecUtils.appendEscape(builder, it))
                    .doOnComplete(() -> builder.append('\''))
                    .then();
            });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ClobValue)) {
                return false;
            }

            ClobValue clobValue = (ClobValue) o;

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
