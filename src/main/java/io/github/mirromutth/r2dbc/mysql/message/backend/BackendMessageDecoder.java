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

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A decoder that reads {@link ByteBuf}s and returns a {@link Flux} of decoded {@link BackendMessage}s.
 */
public final class BackendMessageDecoder implements ByteBufHolder {

    private static final Logger logger = LoggerFactory.getLogger(BackendMessageDecoder.class);

    private final CompositeByteBuf readBuf;
    private final CompositeByteBuf envelopeBuf;

    public BackendMessageDecoder(ByteBufAllocator bufAllocator) {
        this(bufAllocator.compositeBuffer(), bufAllocator.compositeBuffer());
    }

    private BackendMessageDecoder(CompositeByteBuf readBuf, CompositeByteBuf envelopeBuf) {
        this.readBuf = readBuf;
        this.envelopeBuf = envelopeBuf;
    }

    public Flux<BackendMessage> decode(ByteBuf buf, DecodeMode mode) {
        requireNonNull(buf, "buf must not be null");
        requireNonNull(mode, "mode must not be null");

        return Flux.generate(
            () -> this.readBuf.addComponent(true, buf),
            (byteBuf, sink) -> {
                int byteSize = Integer.MAX_VALUE;

                while (byteSize >= ProtocolConstants.MAX_PART_SIZE) {
                    ByteBuf envelopePart = sliceEnvelopePart(byteBuf);

                    if (envelopePart == null) {
                        sink.complete();
                        return byteBuf;
                    }

                    byteSize = envelopePart.readUnsignedMediumLE();
                    envelopePart.skipBytes(1);

                    if (envelopePart.readableBytes() > 0) {
                        this.envelopeBuf.addComponent(true, envelopePart.retain());
                    }
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("Inbound message (header removed):\n{}", ByteBufUtil.prettyHexDump(this.envelopeBuf));
                }

                try {
                    switch (mode) {
                        case HANDSHAKE:
                            sink.next(AbstractHandshakeMessage.decode(this.envelopeBuf));
                            break;
                    }
                } finally {
                    this.envelopeBuf.discardReadComponents();
                }

                return byteBuf;
            },
            CompositeByteBuf::discardReadComponents
        );
    }

    @Nullable
    private ByteBuf sliceEnvelopePart(ByteBuf buf) {
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
        return readBuf;
    }

    @Override
    public ByteBufHolder copy() {
        return replace(readBuf.copy());
    }

    @Override
    public ByteBufHolder duplicate() {
        return replace(readBuf.duplicate());
    }

    @Override
    public ByteBufHolder retainedDuplicate() {
        return replace(readBuf.retainedDuplicate());
    }

    @Override
    public ByteBufHolder replace(ByteBuf byteBuf) {
        CompositeByteBuf newReadBuf = this.readBuf.alloc().compositeBuffer();

        if (byteBuf.isReadable() && byteBuf.readableBytes() > 0) {
            newReadBuf.addComponent(true, byteBuf);
        }

        return new BackendMessageDecoder(newReadBuf, envelopeBufCopy());
    }

    @Override
    public int refCnt() {
        return readBuf.refCnt();
    }

    @Override
    public ByteBufHolder retain() {
        readBuf.retain();
        return this;
    }

    @Override
    public ByteBufHolder retain(int i) {
        readBuf.retain(i);
        return this;
    }

    @Override
    public ByteBufHolder touch() {
        readBuf.touch();
        return this;
    }

    @Override
    public ByteBufHolder touch(Object o) {
        readBuf.touch(o);
        return this;
    }

    @Override
    public boolean release() {
        boolean result = readBuf.release();

        if (result) {
            forceReleaseEnvelope();
        }

        return result;
    }

    @Override
    public boolean release(int i) {
        boolean result = readBuf.release(i);

        if (result) {
            forceReleaseEnvelope();
        }

        return result;
    }

    private void forceReleaseEnvelope() {
        int refCnt = envelopeBuf.refCnt();

        if (refCnt > 0) {
            envelopeBuf.release(refCnt);
        }
    }

    private CompositeByteBuf envelopeBufCopy() {
        CompositeByteBuf envelopeBuf = this.envelopeBuf.alloc().compositeBuffer();

        try {
            envelopeBuf.addComponent(true, this.envelopeBuf.copy());
            return envelopeBuf.retain();
        } finally {
            envelopeBuf.release();
        }
    }
}
