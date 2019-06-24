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

import io.netty.buffer.ByteBuf;

/**
 * EOF error message.
 * <p>
 * Note: EOF message are deprecated and OK message are also used to indicate EOF as of MySQL 5.7.5.
 */
public final class EofMessage implements ServerMessage, WarningMessage {

    private final int warnings;

    private final short serverStatuses;

    private EofMessage(int warnings, short serverStatuses) {
        this.warnings = warnings;
        this.serverStatuses = serverStatuses;
    }

    static EofMessage decode(ByteBuf buf) {
        buf.skipBytes(1); // skip generic header 0xFE of EOF messages
        int warnings = buf.readUnsignedShortLE();
        return new EofMessage(warnings, buf.readShortLE());
    }

    @Override
    public int getWarnings() {
        return warnings;
    }

    public short getServerStatuses() {
        return serverStatuses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EofMessage)) {
            return false;
        }

        EofMessage that = (EofMessage) o;

        if (warnings != that.warnings) {
            return false;
        }
        return serverStatuses == that.serverStatuses;
    }

    @Override
    public int hashCode() {
        int result = warnings;
        result = 31 * result + (int) serverStatuses;
        return result;
    }

    @Override
    public String toString() {
        if (warnings != 0) {
            return String.format("EofMessage{warnings=%d, serverStatuses=%s}", warnings, serverStatuses);
        } else {
            return String.format("EofMessage{serverStatuses=%s}", serverStatuses);
        }
    }
}
