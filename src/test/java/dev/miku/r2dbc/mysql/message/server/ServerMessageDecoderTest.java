/*
 * Copyright 2018-2021 the original author or authors.
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

import dev.miku.r2dbc.mysql.ConnectionContextTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.assertj.core.api.AbstractObjectAssert;
import org.junit.jupiter.api.Test;
import reactor.util.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ServerMessageDecoder}.
 */
class ServerMessageDecoderTest {

    @Test
    void okAndPreparedOk() {
        AbstractObjectAssert<?, OkMessage> ok = assertThat(decode(okLike(), DecodeContext.command()))
            .isExactlyInstanceOf(OkMessage.class)
            .extracting(message -> (OkMessage) message);

        ok.extracting(OkMessage::getAffectedRows).isEqualTo(1L);
        ok.extracting(OkMessage::getLastInsertId).isEqualTo(0x10000L); // 65536
        ok.extracting(OkMessage::getServerStatuses).isEqualTo((short) 0x100); // 256
        ok.extracting(OkMessage::getWarnings).isEqualTo(0);

        AbstractObjectAssert<?, PreparedOkMessage> preparedOk = assertThat(decode(okLike(),
            DecodeContext.prepareQuery()))
            .isExactlyInstanceOf(PreparedOkMessage.class)
            .extracting(message -> (PreparedOkMessage) message);

        preparedOk.extracting(PreparedOkMessage::getStatementId).isEqualTo(0xFD01); // 64769
        preparedOk.extracting(PreparedOkMessage::getTotalColumns).isEqualTo(1);
        preparedOk.extracting(PreparedOkMessage::getTotalParameters).isEqualTo(1);
    }

    @Nullable
    private static ServerMessage decode(ByteBuf buf, DecodeContext decodeContext) {
        return new ServerMessageDecoder().decode(buf, ConnectionContextTest.mock(), decodeContext);
    }

    private static ByteBuf okLike() {
        return Unpooled.wrappedBuffer(new byte[] {
            10, 0, 0, // envelope size
            1, // sequence ID
            0, // Heading both of OK and Prepared OK
            1, // OK: affected rows, Prepared OK: first byte of statement ID
            (byte) 0xFD,
            // OK: VAR_INT_3_BYTE_CODE, last inserted ID is var-int which payload contains 3 bytes
            // Prepared OK: second byte of statement ID
            0, // OK: first byte of last inserted ID payload, Prepared OK: third byte of statement ID
            0, // OK: second byte of last inserted ID payload, Prepared OK: last byte of statement ID
            1, // OK: last byte of last inserted ID payload, Prepared OK: first byte of total columns
            0, // OK: first byte of server statuses, Prepared OK: second byte of total columns
            1, // OK: second byte of server statuses, Prepared OK: first byte of total parameters
            0, // OK: first byte of warnings, Prepared OK: second byte of total parameters
            0 // OK: second byte of warnings, Prepared OK: filter byte for Prepared OK
        });
    }
}
