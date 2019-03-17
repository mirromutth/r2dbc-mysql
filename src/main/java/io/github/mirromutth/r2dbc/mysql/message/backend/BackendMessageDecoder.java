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

import io.github.mirromutth.r2dbc.mysql.constant.CommandType;
import io.github.mirromutth.r2dbc.mysql.constant.ProtocolLifecycle;
import io.github.mirromutth.r2dbc.mysql.constant.ProtocolConstants;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.exception.ProtocolNotSupportException;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A decoder that reads native message packets and returns a {@link Mono} of decoded {@link BackendMessage}.
 */
public final class BackendMessageDecoder {

    private final ByteBufJoiner joiner = ByteBufJoiner.wrapped();

    private final List<ByteBuf> parts = new ArrayList<>();

    @Nullable
    public BackendMessage decode(ByteBuf envelope, ProtocolLifecycle lifecycle, AtomicInteger sequenceId, @Nullable CommandType commandType, MySqlSession session) {
        requireNonNull(lifecycle, "lifecycle must not be null");
        requireNonNull(sequenceId, "sequenceId must not be null");
        requireNonNull(envelope, "envelope must not be null");

        try {
            if (readLastPart(envelope)) {
                sequenceId.set(envelope.readUnsignedByte() + 1);
                ByteBuf joined = joiner.join(parts, envelope);

                envelope = null; // success, no need release

                try {
                    switch (lifecycle) {
                        case CONNECTION:
                            return decodeConnection(joined, session);
                        case COMMAND:
                            requireNonNull(commandType, "commandType must not be null at decoding in command phase");
                            return decodeCommand(joined, commandType, session);
                    }
                } finally {
                    joined.release();
                }

                throw new IllegalStateException("MySQL protocol lifecycle is " + lifecycle + " which is undefined behavior when decoding!");
            } else {
                envelope.skipBytes(1); // sequence Id
                parts.add(envelope);
                envelope = null; // success, no need release
                return null;
            }
        } finally {
            if (envelope != null) {
                envelope.release();
            }
        }
    }

    public void dispose() {
        try {
            for (ByteBuf part : parts) {
                if (part != null) {
                    part.release();
                }
            }
        } finally {
            parts.clear();
        }
    }

    private BackendMessage decodeConnection(ByteBuf buf, MySqlSession session) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case 0: // Ok
                return OkMessage.decode(buf, requireNonNull(session, "session must not be null"));
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
                    // Should decode EOF because connection phase has no completed.
                    return EofMessage.decode(buf);
                }

                return AuthChangeMessage.decode(buf);
        }

        throw new ProtocolNotSupportException("Unknown message header " + header + " on connection phase");
    }

    private BackendMessage decodeCommand(ByteBuf buf, CommandType type, MySqlSession session) {
        switch (type) {
            case STATEMENT_PREPARE:
                // TODO: implement prepare statement
                break;
            case STATEMENT_EXECUTE: case STATEMENT_SIMPLE:
                // TODO: implement result set
                break;
            case UTILITIES_SIMPLE:
                short header = buf.getUnsignedByte(buf.readerIndex());
                switch (header) {
                    case 0: // Ok
                        return OkMessage.decode(buf, requireNonNull(session, "session must not be null"));
                    case 0xFF: // Error
                        return ErrorMessage.decode(buf);
                }

                throw new ProtocolNotSupportException("Unknown message header " + header + " on command phase and command type " + type);
        }

        throw new ProtocolNotSupportException("Unsupported command type " + type);
    }

    private boolean readLastPart(ByteBuf partBuf) {
        int size = partBuf.readUnsignedMediumLE();
        return size < ProtocolConstants.MAX_PART_SIZE;
    }
}
