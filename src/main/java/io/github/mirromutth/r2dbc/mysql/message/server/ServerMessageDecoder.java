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

package io.github.mirromutth.r2dbc.mysql.message.server;

import io.github.mirromutth.r2dbc.mysql.constant.DataValues;
import io.github.mirromutth.r2dbc.mysql.constant.Envelopes;
import io.github.mirromutth.r2dbc.mysql.constant.Headers;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.header.SequenceIdProvider;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Generic message decoder logic.
 */
public final class ServerMessageDecoder {

    private final ByteBufJoiner joiner = ByteBufJoiner.wrapped();

    private final List<ByteBuf> parts = new ArrayList<>();

    @Nullable
    public ServerMessage decode(ByteBuf envelope, SequenceIdProvider sequenceIdProvider, MySqlSession session, DecodeContext decodeContext) {
        requireNonNull(envelope, "envelope must not be null");
        requireNonNull(session, "session must not be null");
        requireNonNull(sequenceIdProvider, "sequenceIdProvider must not be null");

        ByteBuf joined = processEnvelope(envelope, sequenceIdProvider);

        if (joined == null) {
            return null;
        }

        try {
            return decodeMessage(joined, session, decodeContext);
        } finally {
            joined.release();
        }
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void dispose() {
        try {
            // Use the old-style for loop, see: https://github.com/netty/netty/issues/2642
            int size = parts.size();
            for (int i = 0; i < size; ++i) {
                parts.get(i).release();
            }
        } finally {
            parts.clear();
        }
    }

    @Nullable
    private static ServerMessage decodeMessage(ByteBuf joined, MySqlSession session, DecodeContext context) {
        if (context instanceof TextResultDecodeContext) {
            return decodeTextResult(joined, session, (TextResultDecodeContext) context);
        } else if (context instanceof CommandDecodeContext) {
            return decodeCommandMessage(joined, session);
        } else if (context instanceof ConnectionDecodeContext) {
            return decodeConnectionMessage(joined, session);
        }

        throw new IllegalStateException("unknown decode context type: " + context.getClass());
    }

    @Nullable
    private static ServerMessage decodeTextResult(ByteBuf buf, MySqlSession session, TextResultDecodeContext context) {
        short header = buf.getUnsignedByte(buf.readerIndex());

        if (header == Headers.ERROR) {
            // 0xFF is not header of var integer,
            // not header of text result null (0xFB) and
            // not header of column metadata (0x03 + "def")
            return ErrorMessage.decode(buf);
        }

        if (context.isMetadata()) {
            if (ColumnMetadataMessage.isLooksLike(buf)) {
                ColumnMetadataMessage columnMetadata = ColumnMetadataMessage.decode(buf, session.getCollation().getCharset());
                ColumnMetadataMessage[] messages = context.pushAndGetMetadata(columnMetadata);

                if (messages == null) {
                    return null;
                }

                return new FictitiousRowMetadataMessage(messages);
            }
        } else {
            if (header == DataValues.NULL_VALUE) {
                // NULL_VALUE (0xFB) is not header of var integer and not header of OK (0x0 or 0xFE)
                return RowMessage.decode(buf, context);
            } else if (header == Headers.EOF) {
                // 0xFE means it maybe EOF, or var int (64-bits) header.
                int byteSize = buf.readableBytes();

                if (byteSize > 1 + Long.BYTES) {
                    // Maybe var int (64-bits), try to get 64-bits var integer.
                    long minLength = buf.getLongLE(buf.readerIndex() + 1) + 1 + Long.BYTES;
                    // Minimal length for first field with size by var integer encoded.
                    if (buf.readableBytes() >= minLength) {
                        // Huge message, SHOULD NOT be OK message.
                        return RowMessage.decode(buf, context);
                    }
                    // Otherwise it is not a row.
                }

                if (byteSize >= OkMessage.MIN_SIZE) {
                    // Looks like a OK message.
                    return OkMessage.decode(buf, session);
                }
            } else {
                // If header is 0, SHOULD NOT be OK message.
                // Because MySQL server sends OK messages always starting with 0xFE in SELECT statement result.
                return RowMessage.decode(buf, context);
            }
        }

        if ((header == Headers.OK || header == Headers.EOF) && buf.readableBytes() >= OkMessage.MIN_SIZE) {
            return OkMessage.decode(buf, session);
        }

        throw new R2dbcNonTransientResourceException("unknown message header " + header + " on text result phase");
    }

    private static ServerMessage decodeCommandMessage(ByteBuf buf, MySqlSession session) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case Headers.ERROR:
                return ErrorMessage.decode(buf);
            case Headers.OK:
            case Headers.EOF:
                // maybe OK, maybe column count (unsupported EOF on command phase)
                if (buf.readableBytes() >= OkMessage.MIN_SIZE) {
                    // MySQL has hard limit of 4096 columns per-table,
                    // so if readable bytes upper than 7, it means if it is column count,
                    // column count is already upper than (1 << 24) - 1 = 16777215, it is impossible.
                    // So it must be OK message, not be column count.
                    return OkMessage.decode(buf, session);
                }
        }

        if (CodecUtils.checkNextVarInt(buf) == 0) {
            // EOF message must be 5-bytes, it will never be looks like a var integer.
            // It looks like has only a var integer, should be column count.
            return ColumnCountMessage.decode(buf);
        }

        // Always deprecate EOF
        throw new R2dbcNonTransientResourceException("unknown message header " + header + " on command phase");
    }

    private static ServerMessage decodeConnectionMessage(ByteBuf buf, MySqlSession session) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case Headers.OK:
                return OkMessage.decode(buf, requireNonNull(session, "session must not be null"));
            case Headers.AUTH_MORE_DATA: // Auth more data
                return AuthMoreDataMessage.decode(buf);
            case Headers.HANDSHAKE_V9:
            case Headers.HANDSHAKE_V10: // Handshake V9 (not supported) or V10
                return AbstractHandshakeMessage.decode(buf);
            case Headers.ERROR: // Error
                return ErrorMessage.decode(buf);
            case Headers.EOF: // Auth exchange message or EOF message
                int byteSize = buf.readableBytes();

                if (byteSize == 1 || byteSize == 5) { // must be EOF (unsupported EOF 320 message if byte size is 1)
                    // Should decode EOF because connection phase has no completed.
                    return EofMessage.decode(buf);
                }

                return AuthChangeMessage.decode(buf);
        }

        throw new R2dbcPermissionDeniedException("unknown message header " + header + " on connection phase");
    }

    @Nullable
    private ByteBuf processEnvelope(ByteBuf envelope, SequenceIdProvider sequenceIdProvider) {
        try {
            int size = envelope.readUnsignedMediumLE();
            if (size < Envelopes.MAX_PART_SIZE) {
                sequenceIdProvider.last(envelope.readUnsignedByte());
                ByteBuf joined = joiner.join(parts, envelope);
                // success, no need release
                envelope = null;
                return joined;
            } else {
                // skip the sequence Id
                envelope.skipBytes(1);
                parts.add(envelope);
                // success, no need release
                envelope = null;
                return null;
            }
        } finally {
            if (envelope != null) {
                envelope.release();
            }
        }
    }
}
