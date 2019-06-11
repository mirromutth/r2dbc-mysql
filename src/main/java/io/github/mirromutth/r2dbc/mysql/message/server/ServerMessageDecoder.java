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
import io.netty.util.ReferenceCountUtil;
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

    private static final ByteBufJoiner JOINER = ByteBufJoiner.wrapped();

    private final List<ByteBuf> parts = new ArrayList<>();

    @Nullable
    public ServerMessage decode(ByteBuf envelope, SequenceIdProvider sequenceIdProvider, MySqlSession session, DecodeContext decodeContext) {
        requireNonNull(envelope, "envelope must not be null");
        requireNonNull(session, "session must not be null");
        requireNonNull(sequenceIdProvider, "sequenceIdProvider must not be null");

        if (readNotFinish(envelope, sequenceIdProvider)) {
            return null;
        }

        return decodeMessage(parts, session, decodeContext);
    }

    public void dispose() {
        try {
            for (ByteBuf part : parts) {
                ReferenceCountUtil.safeRelease(part);
            }
        } finally {
            parts.clear();
        }
    }

    @Nullable
    private static ServerMessage decodeMessage(List<ByteBuf> buffers, MySqlSession session, DecodeContext context) {
        if (context instanceof ResultDecodeContext) {
            // Maybe very large.
            return decodeResult(buffers, session, (ResultDecodeContext) context);
        }

        ByteBuf joined = JOINER.join(buffers);

        try {
            if (context instanceof PreparedMetadataDecodeContext) {
                return decodePreparedMetadata(joined, session, (PreparedMetadataDecodeContext) context);
            } else if (context instanceof WaitPrepareDecodeContext) {
                return decodeOnWaitPrepare(joined, session);
            } else if (context instanceof CommandDecodeContext) {
                return decodeCommandMessage(joined, session);
            } else if (context instanceof ConnectionDecodeContext) {
                return decodeConnectionMessage(joined, session);
            }
        } finally {
            joined.release();
        }

        throw new IllegalStateException("unknown decode context type: " + context.getClass());
    }

    @Nullable
    private static ServerMessage decodePreparedMetadata(ByteBuf buf, MySqlSession session, PreparedMetadataDecodeContext context) {
        short header = buf.getUnsignedByte(buf.readerIndex());

        if (header == Headers.ERROR) {
            // 0xFF is not header of var integer,
            // not header of text result null (0xFB) and
            // not header of column metadata (0x03 + "def")
            return ErrorMessage.decode(buf);
        }

        if (context.isMetadata() && DefinitionMetadataMessage.isLooksLike(buf)) {
            return decodeMetadata(buf, session, context);
        }

        throw new R2dbcNonTransientResourceException("unknown message header " + header + " on prepared metadata phase");
    }

    @Nullable
    private static ServerMessage decodeResult(List<ByteBuf> buffers, MySqlSession session, ResultDecodeContext context) {
        ByteBuf firstBuf = buffers.get(0);
        short header = firstBuf.getUnsignedByte(firstBuf.readerIndex());

        if (header == Headers.ERROR) {
            // 0xFF is not header of var integer,
            // not header of text result null (0xFB) and
            // not header of column metadata (0x03 + "def")
            ByteBuf joined = JOINER.join(buffers);
            try {
                return ErrorMessage.decode(joined);
            } finally {
                joined.release();
            }
        }

        if (context.isMetadata()) {
            if (DefinitionMetadataMessage.isLooksLike(firstBuf)) {
                ByteBuf joined = JOINER.join(buffers);
                try {
                    return decodeMetadata(joined, session, context);
                } finally {
                    joined.release();
                }
            }
        } else {
            if (context.isBinary()) {
                if (header == Headers.OK) {
                    // If header is 0, SHOULD NOT be OK message.
                    // Because MySQL server sends OK messages always starting with 0xFE in SELECT statement result.
                    try (FieldReader reader = FieldReader.of(JOINER, buffers)) {
                        return BinaryRowMessage.decode(reader, context);
                    }
                }
            } else if (isTextRow(buffers, firstBuf, header)) {
                try (FieldReader reader = FieldReader.of(JOINER, buffers)) {
                    return TextRowMessage.decode(reader, context.getTotalColumns());
                }
            }
        }

        if ((header == Headers.OK || header == Headers.EOF) && firstBuf.readableBytes() >= OkMessage.MIN_SIZE) {
            ByteBuf joined = JOINER.join(buffers);

            try {
                return OkMessage.decode(joined, session);
            } finally {
                joined.release();
            }
        }

        try {
            for (ByteBuf buffer : buffers) {
                ReferenceCountUtil.safeRelease(buffer);
            }
        } finally {
            buffers.clear();
        }

        throw new R2dbcNonTransientResourceException(String.format("unknown message header %d on %s result phase", header, context.isBinary() ? "binary" : "text"));
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
                break;
        }

        if (CodecUtils.checkNextVarInt(buf) == 0) {
            // EOF message must be 5-bytes, it will never be looks like a var integer.
            // It looks like has only a var integer, should be column count.
            return ColumnCountMessage.decode(buf);
        }

        // Always deprecate EOF
        throw new R2dbcNonTransientResourceException("unknown message header " + header + " on command phase");
    }

    private static ServerMessage decodeOnWaitPrepare(ByteBuf buf, MySqlSession session) {
        short header = buf.getUnsignedByte(buf.readerIndex());

        if (Headers.ERROR == header) {
            return ErrorMessage.decode(buf);
        } else if (Headers.OK == header) {
            // should be prepared ok, but test in here...
            if (PreparedOkMessage.isLooksLike(buf)) {
                return PreparedOkMessage.decode(buf);
            } else if (buf.readableBytes() >= OkMessage.MIN_SIZE) {
                return OkMessage.decode(buf, session);
            }
        } else if (Headers.EOF == header && buf.readableBytes() >= OkMessage.MIN_SIZE) {
            return OkMessage.decode(buf, session);
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
    private boolean readNotFinish(ByteBuf envelope, SequenceIdProvider sequenceIdProvider) {
        try {
            int size = envelope.readUnsignedMediumLE();
            if (size < Envelopes.MAX_ENVELOPE_SIZE) {
                sequenceIdProvider.last(envelope.readUnsignedByte());
                parts.add(envelope);
                // success, no need release
                envelope = null;
                return false;
            } else {
                // skip the sequence Id
                envelope.skipBytes(1);
                parts.add(envelope);
                // success, no need release
                envelope = null;
                return true;
            }
        } finally {
            if (envelope != null) {
                envelope.release();
            }
        }
    }

    private static boolean isTextRow(List<ByteBuf> buffers, ByteBuf firstBuf, short header) {
        if (header == DataValues.NULL_VALUE) {
            // NULL_VALUE (0xFB) is not header of var integer and not header of OK (0x0 or 0xFE)
            return true;
        } else if (header == Headers.EOF) {
            // 0xFE means it maybe EOF, or var int (64-bits) header.
            long allBytes = firstBuf.readableBytes();

            if (allBytes > Byte.BYTES + Long.BYTES) {
                long needBytes = firstBuf.getLongLE(firstBuf.readerIndex() + Byte.BYTES) + Byte.BYTES + Long.BYTES;
                // Maybe var int (64-bits), try to get 64-bits var integer.
                // Minimal length for first field with size by var integer encoded.
                // Should not be OK message if it is big message.
                if (allBytes >= needBytes) {
                    return true;
                }

                int size = buffers.size();
                for (int i = 1; i < size; ++i) {
                    allBytes += buffers.get(i).readableBytes();

                    if (allBytes >= needBytes) {
                        return true;
                    }
                }

                return false;
            } else {
                // Is not a var integer, it is not text row.
                return false;
            }
        } else {
            // If header is 0, SHOULD NOT be OK message.
            // Because MySQL server sends OK messages always starting with 0xFE in SELECT statement result.
            // Now, it is not OK message, not be error message, it must be text row.
            return true;
        }
    }

    @Nullable
    private static FakeRowMetadataMessage decodeMetadata(ByteBuf buf, MySqlSession session, MetadataDecodeContext context) {
        DefinitionMetadataMessage columnMetadata = DefinitionMetadataMessage.decode(buf, session.getCollation().getCharset());
        DefinitionMetadataMessage[] messages = context.pushAndGetMetadata(columnMetadata);

        if (messages == null) {
            return null;
        }

        return new FakeRowMetadataMessage(messages);
    }
}
