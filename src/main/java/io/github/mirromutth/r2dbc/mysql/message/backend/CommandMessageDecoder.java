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

import io.github.mirromutth.r2dbc.mysql.constant.ProtocolPhase;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.exception.ProtocolNotSupportException;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;

import static io.github.mirromutth.r2dbc.mysql.constant.DataValues.NULL_VALUE;

/**
 * A message decoder for command phase.
 */
final class CommandMessageDecoder extends AbstractMessageDecoder {

    @Override
    BackendMessage decodeMessage(ByteBuf joined, DecodeContext context, MySqlSession session) {
        if (context instanceof TextResultDecodeContext) {
            TextResultDecodeContext ctx = (TextResultDecodeContext) context;

            short header = joined.getUnsignedByte(joined.readerIndex());

            if (header == 0xFF) {
                // 0xFF is not header of var integer,
                // not header of text result null (0xFB) and
                // not header of column metadata (0x03 + "def")
                return ErrorMessage.decode(joined);
            }

            if (ctx.isInitialized()) {
                if (ctx.isMetadata()) {
                    if (ColumnMetadataMessage.isLooksLike(joined)) {
                        return ColumnMetadataMessage.decode(joined, ctx.nextColumnId(), session.getCollation().getCharset());
                    }
                } else {
                    if (header == NULL_VALUE) { // NULL_VALUE (0xFB) is not header of var integer and not header of OK (0x0 or 0xFE)
                        return RowMessage.decode(joined, ctx);
                    }
                }
            } else if (CodecUtils.checkNextVarInt(joined) == 0) {
                ctx.initialize(CodecUtils.readVarInt(joined));
                return null;
            }
        }

        return decodeDefaultMessage(joined, session);
    }

    private BackendMessage decodeDefaultMessage(ByteBuf joined, MySqlSession session) {
        short header = joined.getUnsignedByte(joined.readerIndex());
        switch (header) {
            case 0: // Ok
                return OkMessage.decode(joined, session);
            case 0xFE: // maybe OK, maybe EOF (unsupported EOF on command phase)
                if (joined.readableBytes() >= OkMessage.MIN_SIZE) {
                    return OkMessage.decode(joined, session);
                }

                break;
            case 0xFF: // Error
                return ErrorMessage.decode(joined);
        }

        // Always deprecate EOF
        throw new ProtocolNotSupportException("Unknown message header " + header + " on command phase");
    }

    @Override
    public ProtocolPhase protocolPhase() {
        return ProtocolPhase.COMMAND;
    }
}
