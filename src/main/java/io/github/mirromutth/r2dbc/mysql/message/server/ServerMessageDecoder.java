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
import io.github.mirromutth.r2dbc.mysql.internal.ConnectionContext;
import io.github.mirromutth.r2dbc.mysql.message.header.SequenceIdProvider;
import io.github.mirromutth.r2dbc.mysql.internal.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * Generic message decoder logic.
 */
public final class ServerMessageDecoder {

    private static final ByteBufJoiner JOINER = ByteBufJoiner.wrapped();

    private final List<ByteBuf> parts = new ArrayList<>();

    @Nullable
    public ServerMessage decode(ByteBuf envelope, ConnectionContext context, DecodeContext decodeContext, @Nullable SequenceIdProvider.Linkable idProvider) {
        requireNonNull(envelope, "envelope must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(decodeContext, "decodeContext must not be null");

        if (readNotFinish(envelope, idProvider)) {
            return null;
        }

        return decodeMessage(parts, context, decodeContext);
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
    private static ServerMessage decodeMessage(List<ByteBuf> buffers, ConnectionContext context, DecodeContext decodeContext) {
        if (decodeContext instanceof ResultDecodeContext) {
            // Maybe very large.
            return decodeResult(buffers, context, (ResultDecodeContext) decodeContext);
        }

        ByteBuf joined = JOINER.join(buffers);

        try {
            if (decodeContext instanceof PreparedMetadataDecodeContext) {
                return decodePreparedMetadata(joined, context, (PreparedMetadataDecodeContext) decodeContext);
            } else if (decodeContext instanceof CommandDecodeContext) {
                return decodeCommandMessage(joined, context);
            } else if (decodeContext instanceof ConnectionDecodeContext) {
                return decodeConnectionMessage(joined, context);
            }
        } finally {
            joined.release();
        }

        throw new IllegalStateException("unknown decode context type: " + decodeContext.getClass());
    }

    @Nullable
    private static ServerMessage decodePreparedMetadata(ByteBuf buf, ConnectionContext context, PreparedMetadataDecodeContext decodeContext) {
        short header = buf.getUnsignedByte(buf.readerIndex());

        if (header == Headers.ERROR) {
            // 0xFF is not header of var integer,
            // not header of text result null (0xFB) and
            // not header of column metadata (0x03 + "def")
            return ErrorMessage.decode(buf);
        }

        if (decodeContext.isInMetadata()) {
            return decodeInMetadata(buf, header, context, decodeContext);
        }

        throw new R2dbcNonTransientResourceException(String.format("Unknown message header 0x%x and readable bytes is %d on prepared metadata phase", header, buf.readableBytes()));
    }

    @Nullable
    private static ServerMessage decodeResult(List<ByteBuf> buffers, ConnectionContext context, ResultDecodeContext decodeContext) {
        ByteBuf firstBuf = buffers.get(0);
        short header = firstBuf.getUnsignedByte(firstBuf.readerIndex());

        if (Headers.ERROR == header) {
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

        if (decodeContext.isInMetadata()) {
            ByteBuf joined = JOINER.join(buffers);
            try {
                return decodeInMetadata(joined, header, context, decodeContext);
            } finally {
                joined.release();
            }
            // Should not has other messages when metadata reading.
        }

        if (isRow(buffers, firstBuf, header)) {
            return new RowMessage(FieldReader.of(JOINER, buffers));
        } else if (header == Headers.EOF) {
            int byteSize = firstBuf.readableBytes();

            if (OkMessage.isValidSize(byteSize)) {
                ByteBuf joined = JOINER.join(buffers);

                try {
                    return OkMessage.decode(joined, context);
                } finally {
                    joined.release();
                }
            } else if (EofMessage.isValidSize(byteSize)) {
                ByteBuf joined = JOINER.join(buffers);

                try {
                    return EofMessage.decode(joined);
                } finally {
                    joined.release();
                }
            }
        }

        long totalBytes = 0;
        try {
            for (ByteBuf buffer : buffers) {
                totalBytes += buffer.readableBytes();
                ReferenceCountUtil.safeRelease(buffer);
            }
        } finally {
            buffers.clear();
        }

        throw new R2dbcNonTransientResourceException(String.format("Unknown message header 0x%x and readable bytes is %d on result phase", header, totalBytes));
    }

    private static ServerMessage decodeCommandMessage(ByteBuf buf, ConnectionContext context) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case Headers.ERROR:
                return ErrorMessage.decode(buf);
            case Headers.OK:
                if (PreparedOkMessage.isLooksLike(buf)) {
                    return PreparedOkMessage.decode(buf);
                } else if (OkMessage.isValidSize(buf.readableBytes())) {
                    return OkMessage.decode(buf, context);
                }

                break;
            case Headers.EOF:
                int byteSize = buf.readableBytes();

                // Maybe OK, maybe column count (unsupported EOF on command phase)
                if (OkMessage.isValidSize(byteSize)) {
                    // MySQL has hard limit of 4096 columns per-table,
                    // so if readable bytes upper than 7, it means if it is column count,
                    // column count is already upper than (1 << 24) - 1 = 16777215, it is impossible.
                    // So it must be OK message, not be column count.
                    return OkMessage.decode(buf, context);
                } else if (EofMessage.isValidSize(byteSize)) {
                    return EofMessage.decode(buf);
                }
        }

        if (CodecUtils.checkNextVarInt(buf) == 0) {
            // EOF message must be 5-bytes, it will never be looks like a var integer.
            // It looks like has only a var integer, should be column count.
            return ColumnCountMessage.decode(buf);
        }

        throw new R2dbcNonTransientResourceException(String.format("Unknown message header 0x%x and readable bytes is %d on command phase", header, buf.readableBytes()));
    }

    private static ServerMessage decodeConnectionMessage(ByteBuf buf, ConnectionContext context) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case Headers.OK:
                if (OkMessage.isValidSize(buf.readableBytes())) {
                    return OkMessage.decode(buf, context);
                }

                break;
            case Headers.AUTH_MORE_DATA: // Auth more data
                return AuthMoreDataMessage.decode(buf);
            case Headers.HANDSHAKE_V9:
            case Headers.HANDSHAKE_V10: // Handshake V9 (not supported) or V10
                return HandshakeRequest.decode(buf);
            case Headers.ERROR: // Error
                return ErrorMessage.decode(buf);
            case Headers.EOF: // Auth exchange message or EOF message
                if (EofMessage.isValidSize(buf.readableBytes())) {
                    return EofMessage.decode(buf);
                } else {
                    return AuthChangeMessage.decode(buf);
                }
        }

        throw new R2dbcPermissionDeniedException(String.format("Unknown message header 0x%x and readable bytes is %d on connection phase", header, buf.readableBytes()));
    }

    private boolean readNotFinish(ByteBuf envelope, @Nullable SequenceIdProvider.Linkable idProvider) {
        try {
            int size = envelope.readUnsignedMediumLE();
            if (size < Envelopes.MAX_ENVELOPE_SIZE) {
                if (idProvider == null) {
                    // Just ignore sequence id because of no need link any message.
                    envelope.skipBytes(1);
                } else {
                    // Link last message.
                    idProvider.last(envelope.readUnsignedByte());
                }

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

    private static boolean isRow(List<ByteBuf> buffers, ByteBuf firstBuf, short header) {
        if (header == DataValues.NULL_VALUE) {
            // NULL_VALUE (0xFB) is not header of var integer and not header of OK (0x0 or 0xFE)
            return true;
        } else if (header == Headers.EOF) {
            // 0xFE means it maybe EOF, or var int (64-bits) header in text row.
            if (buffers.size() > 1) {
                // Multi-buffers, must be big data row message.
                return true;
            } else {
                // Not EOF or OK.
                int size = firstBuf.readableBytes();
                return !EofMessage.isValidSize(size) && !OkMessage.isValidSize(size);
            }
        } else {
            // If header is 0, SHOULD NOT be OK message.
            // Because MySQL server sends OK messages always starting with 0xFE in SELECT statement result.
            // Now, it is not OK message, not be error message, it must be row.
            return true;
        }
    }

    @Nullable
    private static SyntheticMetadataMessage decodeInMetadata(ByteBuf buf, short header, ConnectionContext context, MetadataDecodeContext decodeContext) {
        ServerMessage message;

        if (Headers.EOF == header && EofMessage.isValidSize(buf.readableBytes())) {
            message = EofMessage.decode(buf);
        } else if (DefinitionMetadataMessage.isLooksLike(buf)) {
            message = DefinitionMetadataMessage.decode(buf, context.getCollation().getCharset());
        } else {
            throw new R2dbcNonTransientResourceException(String.format("Unknown message header 0x%x and readable bytes is %d when reading metadata", header, buf.readableBytes()));
        }

        return decodeContext.putPart(message);
    }
}
