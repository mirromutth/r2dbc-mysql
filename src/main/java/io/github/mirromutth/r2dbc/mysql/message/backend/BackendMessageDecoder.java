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

package io.github.mirromutth.r2dbc.mysql.message.backend;

import io.github.mirromutth.r2dbc.mysql.constant.DecodeMode;
import io.github.mirromutth.r2dbc.mysql.constant.ProtocolConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * A decoder that reads {@link ByteBuf}s and returns a {@link Flux} of decoded {@link BackendMessage}s.
 */
public final class BackendMessageDecoder implements ByteBufHolder {

    private static final Logger logger = LoggerFactory.getLogger(BackendMessageDecoder.class);

    private final CompositeByteBuf byteBuf;

    public BackendMessageDecoder(ByteBufAllocator bufAllocator) {
        this(bufAllocator.compositeBuffer());
    }

    private BackendMessageDecoder(CompositeByteBuf byteBuf) {
        this.byteBuf = requireNonNull(byteBuf, "byteBuf must not be null");
    }

    public Flux<BackendMessage> decode(ByteBuf buf, DecodeMode mode) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(mode, "mode must not be null");

        return Flux.generate(
            () -> this.byteBuf.addComponent(true, buf),
            (byteBuf, sink) -> {
                ByteBuf envelope = sliceEnvelope(byteBuf);

                if (envelope == null) {
                    sink.complete();
                    return byteBuf;
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("inbound message:\n{}", ByteBufUtil.prettyHexDump(envelope));
                }

                envelope.skipBytes(ProtocolConstants.ENVELOPE_HEADER_SIZE);

                switch (mode) {
                    case HANDSHAKE:
                        sink.next(AbstractHandshakeMessage.decode(envelope));
                        break;
                }

                return byteBuf;
            },
            CompositeByteBuf::discardReadComponents
        );
    }

    @Nullable
    private ByteBuf sliceEnvelope(ByteBuf buf) {
        if (buf.readableBytes() < ProtocolConstants.ENVELOPE_HEADER_SIZE) {
            return null;
        }

        int envelopeSize = buf.getUnsignedMediumLE(buf.readerIndex()) + ProtocolConstants.ENVELOPE_HEADER_SIZE;

        if (buf.readableBytes() < envelopeSize) {
            return null;
        }

        return buf.readSlice(envelopeSize);
    }

    @Override
    public ByteBuf content() {
        return byteBuf;
    }

    @Override
    public ByteBufHolder copy() {
        return replace(byteBuf.copy());
    }

    @Override
    public ByteBufHolder duplicate() {
        return replace(byteBuf.duplicate());
    }

    @Override
    public ByteBufHolder retainedDuplicate() {
        return replace(byteBuf.retainedDuplicate());
    }

    @Override
    public ByteBufHolder replace(ByteBuf byteBuf) {
        CompositeByteBuf newBuf = this.byteBuf.alloc().compositeBuffer();

        if (byteBuf.isReadable() && byteBuf.readableBytes() > 0) {
            newBuf.addComponent(true, byteBuf);
        }

        return new BackendMessageDecoder(newBuf.addComponent(true, byteBuf));
    }

    @Override
    public int refCnt() {
        return byteBuf.refCnt();
    }

    @Override
    public ByteBufHolder retain() {
        byteBuf.retain();
        return this;
    }

    @Override
    public ByteBufHolder retain(int i) {
        byteBuf.retain(i);
        return this;
    }

    @Override
    public ByteBufHolder touch() {
        byteBuf.touch();
        return this;
    }

    @Override
    public ByteBufHolder touch(Object o) {
        byteBuf.touch(o);
        return this;
    }

    @Override
    public boolean release() {
        return byteBuf.release();
    }

    @Override
    public boolean release(int i) {
        return byteBuf.release(i);
    }
}
