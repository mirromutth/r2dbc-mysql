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
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A decoder that reads native message packets and returns a {@link Mono} of
 * decoded {@link BackendMessage} at connection phase.
 */
final class ConnectionMessageDecoder extends AbstractMessageDecoder {

    @NonNull
    @Override
    BackendMessage decodeMessage(ByteBuf buf, DecodeContext context, MySqlSession session) {
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

    @Override
    public ProtocolPhase protocolPhase() {
        return ProtocolPhase.CONNECTION;
    }
}
