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

package dev.miku.r2dbc.mysql.message.server;

import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.constant.Envelopes;
import dev.miku.r2dbc.mysql.util.NettyBufferUtils;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Generic message decoder logic.
 */
public final class ServerMessageDecoder {

    private static final short OK = 0;

    private static final short AUTH_MORE_DATA = 1;

    private static final short HANDSHAKE_V9 = 9;

    private static final short HANDSHAKE_V10 = 10;

    private static final short EOF = 0xFE;

    private static final short ERROR = 0xFF;

    private final List<ByteBuf> parts = new ArrayList<>();

    /**
     * Decode a server-side message from {@link #parts} and current envelope.
     *
     * @param envelope      the current envelope.
     * @param context       the connection context.
     * @param decodeContext the decode context.
     * @return the server-side message, or {@code null} if {@code envelope} is not last packet.
     */
    @Nullable
    public ServerMessage decode(ByteBuf envelope, ConnectionContext context, DecodeContext decodeContext) {
        requireNonNull(envelope, "envelope must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(decodeContext, "decodeContext must not be null");

        List<ByteBuf> buffers = this.parts;
        Byte id = readNotFinish(buffers, envelope);
        if (id == null) {
            return null;
        }

        return decodeMessage(buffers, id.intValue() & 0xFF, context, decodeContext);
    }

    /**
     * Dispose the underlying resource.
     */
    public void dispose() {
        if (parts.isEmpty()) {
            return;
        }

        NettyBufferUtils.releaseAll(parts);
        parts.clear();
    }

    @Nullable
    private static ServerMessage decodeMessage(List<ByteBuf> buffers, int envelopeId,
        ConnectionContext context, DecodeContext decodeContext) {
        if (decodeContext instanceof ResultDecodeContext) {
            return decodeResult(buffers, context, (ResultDecodeContext) decodeContext);
        } else if (decodeContext instanceof FetchDecodeContext) {
            return decodeFetch(buffers, context);
        }

        ByteBuf combined = ByteBufCombiner.composite(buffers);

        try {
            if (decodeContext instanceof CommandDecodeContext) {
                return decodeCommandMessage(combined, context);
            } else if (decodeContext instanceof PreparedMetadataDecodeContext) {
                return decodePreparedMetadata(combined, context,
                    (PreparedMetadataDecodeContext) decodeContext);
            } else if (decodeContext instanceof PrepareQueryDecodeContext) {
                return decodePrepareQuery(combined);
            } else if (decodeContext instanceof LoginDecodeContext) {
                return decodeLogin(envelopeId, combined, context);
            }
        } finally {
            combined.release();
        }

        throw new IllegalStateException("unknown decode context type: " + decodeContext.getClass());
    }

    @Nullable
    private static ServerMessage decodePreparedMetadata(ByteBuf buf, ConnectionContext context,
        PreparedMetadataDecodeContext decodeContext) {
        short header = buf.getUnsignedByte(buf.readerIndex());

        if (header == ERROR) {
            // 0xFF is not header of var integer,
            // not header of text result null (0xFB) and
            // not header of column metadata (0x03 + "def")
            return ErrorMessage.decode(buf);
        }

        if (decodeContext.isInMetadata()) {
            return decodeInMetadata(buf, header, context, decodeContext);
        }

        throw new R2dbcNonTransientResourceException("Unknown message header 0x" +
            Integer.toHexString(header) + " and readable bytes is " + buf.readableBytes() +
            " on prepared metadata phase");
    }

    private static ServerMessage decodeFetch(List<ByteBuf> buffers, ConnectionContext context) {
        ByteBuf firstBuf = buffers.get(0);
        short header = firstBuf.getUnsignedByte(firstBuf.readerIndex());
        ErrorMessage error = decodeCheckError(buffers, header);

        if (error != null) {
            return error;
        }

        return decodeRow(buffers, firstBuf, header, context, "fetch");
    }

    @Nullable
    private static ServerMessage decodeResult(List<ByteBuf> buffers, ConnectionContext context,
        ResultDecodeContext decodeContext) {
        ByteBuf firstBuf = buffers.get(0);
        short header = firstBuf.getUnsignedByte(firstBuf.readerIndex());
        ErrorMessage error = decodeCheckError(buffers, header);

        if (error != null) {
            return error;
        }

        if (decodeContext.isInMetadata()) {
            ByteBuf combined = ByteBufCombiner.composite(buffers);
            try {
                return decodeInMetadata(combined, header, context, decodeContext);
            } finally {
                combined.release();
            }
            // Should not has other messages when metadata reading.
        }

        return decodeRow(buffers, firstBuf, header, context, "result");
    }

    private static ServerMessage decodePrepareQuery(ByteBuf buf) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case ERROR:
                return ErrorMessage.decode(buf);
            case OK:
                if (PreparedOkMessage.isLooksLike(buf)) {
                    return PreparedOkMessage.decode(buf);
                }
                break;
        }

        throw new R2dbcNonTransientResourceException("Unknown message header 0x" +
            Integer.toHexString(header) + " and readable bytes is " + buf.readableBytes() +
            " on prepare query phase");
    }

    private static ServerMessage decodeCommandMessage(ByteBuf buf, ConnectionContext context) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case ERROR:
                return ErrorMessage.decode(buf);
            case OK:
                if (OkMessage.isValidSize(buf.readableBytes())) {
                    return OkMessage.decode(buf, context);
                }

                break;
            case EOF:
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

        if (VarIntUtils.checkNextVarInt(buf) == 0) {
            // EOF message must be 5-bytes, it will never be looks like a var integer.
            // It looks like has only a var integer, should be column count.
            return ColumnCountMessage.decode(buf);
        }

        throw new R2dbcNonTransientResourceException("Unknown message header 0x" +
            Integer.toHexString(header) + " and readable bytes is " + buf.readableBytes() +
            " on command phase");
    }

    private static ServerMessage decodeLogin(int envelopeId, ByteBuf buf, ConnectionContext context) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case OK:
                if (OkMessage.isValidSize(buf.readableBytes())) {
                    return OkMessage.decode(buf, context);
                }

                break;
            case AUTH_MORE_DATA: // Auth more data
                return AuthMoreDataMessage.decode(envelopeId, buf);
            case HANDSHAKE_V9:
            case HANDSHAKE_V10: // Handshake V9 (not supported) or V10
                return HandshakeRequest.decode(envelopeId, buf);
            case ERROR: // Error
                return ErrorMessage.decode(buf);
            case EOF: // Auth exchange message or EOF message
                if (EofMessage.isValidSize(buf.readableBytes())) {
                    return EofMessage.decode(buf);
                }

                return ChangeAuthMessage.decode(envelopeId, buf);
        }

        throw new R2dbcPermissionDeniedException("Unknown message header 0x" +
            Integer.toHexString(header) + " and readable bytes is " + buf.readableBytes() +
            " on connection phase");
    }

    @Nullable
    private static Byte readNotFinish(List<ByteBuf> buffers, ByteBuf envelope) {
        try {
            int size = envelope.readUnsignedMediumLE();
            if (size < Envelopes.MAX_ENVELOPE_SIZE) {
                Byte envelopeId = envelope.readByte();

                buffers.add(envelope);
                // success, no need release
                envelope = null;
                return envelopeId;
            }

            // skip the sequence Id
            envelope.skipBytes(1);
            buffers.add(envelope);
            // success, no need release
            envelope = null;
            return null;
        } finally {
            if (envelope != null) {
                envelope.release();
            }
        }
    }

    private static boolean isRow(List<ByteBuf> buffers, ByteBuf firstBuf, short header) {
        switch (header) {
            case RowMessage.NULL_VALUE:
                // NULL_VALUE (0xFB) is not header of var integer and not header of OK (0x0 or 0xFE)
                return true;
            case EOF:
                // 0xFE means it maybe EOF, or var int (64-bits) header in text row.
                if (buffers.size() > 1) {
                    // Multi-buffers, must be big data row message.
                    return true;
                }

                // Not EOF or OK.
                int size = firstBuf.readableBytes();
                return !EofMessage.isValidSize(size) && !OkMessage.isValidSize(size);
            default:
                // If header is 0, SHOULD NOT be OK message.
                // Because MySQL sends OK messages always starting with 0xFE in SELECT statement result.
                // Now, it is not OK message, not be error message, it must be row.
                return true;
        }
    }

    @Nullable
    private static ErrorMessage decodeCheckError(List<ByteBuf> buffers, short header) {
        if (ERROR == header) {
            // 0xFF is not header of var integer,
            // not header of text result null (0xFB) and
            // not header of column metadata (0x03 + "def")
            ByteBuf combined = ByteBufCombiner.composite(buffers);
            try {
                return ErrorMessage.decode(combined);
            } finally {
                combined.release();
            }
        }

        return null;
    }

    private static ServerMessage decodeRow(List<ByteBuf> buffers, ByteBuf firstBuf, short header,
        ConnectionContext context, String phase) {
        if (isRow(buffers, firstBuf, header)) {
            // FieldReader will clear the buffers.
            return new RowMessage(FieldReader.of(buffers));
        } else if (header == EOF) {
            int byteSize = firstBuf.readableBytes();

            if (OkMessage.isValidSize(byteSize)) {
                ByteBuf combined = ByteBufCombiner.composite(buffers);

                try {
                    return OkMessage.decode(combined, context);
                } finally {
                    combined.release();
                }
            } else if (EofMessage.isValidSize(byteSize)) {
                ByteBuf combined = ByteBufCombiner.composite(buffers);

                try {
                    return EofMessage.decode(combined);
                } finally {
                    combined.release();
                }
            }
        }

        long totalBytes = 0;
        try {
            for (ByteBuf buffer : buffers) {
                if (buffer != null) {
                    totalBytes += buffer.readableBytes();
                    ReferenceCountUtil.safeRelease(buffer);
                }
            }
        } finally {
            buffers.clear();
        }

        throw new R2dbcNonTransientResourceException("Unknown message header 0x" +
            Integer.toHexString(header) + " and readable bytes is " + totalBytes + " on " + phase + " phase");
    }

    @Nullable
    private static SyntheticMetadataMessage decodeInMetadata(ByteBuf buf, short header,
        ConnectionContext context, MetadataDecodeContext decodeContext) {
        ServerMessage message;

        if (EOF == header && EofMessage.isValidSize(buf.readableBytes())) {
            message = EofMessage.decode(buf);
        } else {
            message = DefinitionMetadataMessage.decode(buf, context);
        }

        if (message instanceof ServerStatusMessage) {
            context.setServerStatuses(((ServerStatusMessage) message).getServerStatuses());
        }

        return decodeContext.putPart(message);
    }
}
