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
import io.github.mirromutth.r2dbc.mysql.core.ServerSession;
import io.github.mirromutth.r2dbc.mysql.exception.ProtocolNotSupportException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A decoder that reads native message packets and returns a {@link Mono} of decoded {@link BackendMessage}.
 */
public final class BackendMessageDecoder {

    private final AtomicInteger sequenceId;

    private final ByteBufJoiner joiner;

    private final List<ByteBuf> parts = new ArrayList<>();

    private volatile DecodeMode decodeMode = DecodeMode.CONNECTION;

    private volatile ServerSession serverSession = null;

    public BackendMessageDecoder(ByteBufAllocator bufAllocator, AtomicInteger sequenceId) {
        this.joiner = ByteBufJoiner.composite(requireNonNull(bufAllocator, "bufAllocator must not be null"));
        this.sequenceId = requireNonNull(sequenceId, "sequenceId must not be null");
    }

    public Mono<BackendMessage> decode(ByteBuf buf) {
        try {
            if (readLastPart(buf)) {
                sequenceId.set(buf.readUnsignedByte() + 1);
                ByteBuf joined = joiner.join(parts, buf);
                try {
                    buf = null;
                    parts.clear();

                    switch (decodeMode) {
                        case CONNECTION:
                            return Mono.defer(() -> Mono.just(decodeConnection(joined)));
                        case COMMAND:
                            return Mono.empty();
                        case REPLICATION:
                            return Mono.empty();
                    }

                    return Mono.empty();
                } finally {
                    joined.release();
                }
            } else {
                buf.skipBytes(1); // sequence Id
                parts.add(buf);
                buf = null;
                return Mono.empty();
            }
        } finally {
            if (buf != null) {
                buf.release();
            }
        }
    }

    public void initServerSession(ServerSession serverSession) {
        if (this.serverSession == null) {
            this.serverSession = requireNonNull(serverSession, "serverSession must not be null");
        }
    }

    public void release() {
        try {
            for (ByteBuf part : parts) {
                part.release();
            }
        } finally {
            parts.clear();
        }
    }

    private BackendMessage decodeConnection(ByteBuf buf) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case 0: // Ok
                try {
                    return OkMessage.decode(buf, serverSession);
                } finally {
                    this.decodeMode = DecodeMode.COMMAND; // connection phase has completed
                }
            case 1: // Auth more data
                return AuthMoreDataMessage.decode(buf);
            case 9:
            case 10: // Handshake V9 (not supported) or V10
                return AbstractHandshakeMessage.decode(buf);
            case 0xFF: // Error
                return ErrorMessage.decode(buf);
            case 0xFE: // Auth exchange message or EOF message
                int byteSize = buf.readableBytes();

                if (byteSize == 1 || byteSize == 5) { // must be EOF (unsupported EOF 320 message if byte size is 1)
                    return EofMessage.decode(buf);
                }

                return AuthChangeMessage.decode(buf);
        }

        throw new ProtocolNotSupportException("Unknown message header " + header + " on connection phase");
    }

    private boolean readLastPart(ByteBuf partBuf) {
        int size = partBuf.readUnsignedMediumLE();
        return size < ProtocolConstants.MAX_PART_SIZE;
    }
}
