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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.Capability;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Map;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;

/**
 * An abstraction of {@link LoginClientMessage} considers handshake response.
 */
public interface HandshakeResponse extends LoginClientMessage {

    static HandshakeResponse from(
        int envelopeId, Capability capability, int collationId, String user, byte[] authentication,
        String authType, String database, Map<String, String> attributes
    ) {
        if (capability.isProtocol41()) {
            return new HandshakeResponse41(envelopeId, capability, collationId, user, authentication, authType, database, attributes);
        } else {
            return new HandshakeResponse320(envelopeId, capability, user, authentication, database);
        }
    }

    static void writeCString(ByteBuf buf, String value, Charset charset) {
        if (!value.isEmpty()) {
            buf.writeCharSequence(value, charset);
        }
        buf.writeByte(TERMINAL);
    }
}
