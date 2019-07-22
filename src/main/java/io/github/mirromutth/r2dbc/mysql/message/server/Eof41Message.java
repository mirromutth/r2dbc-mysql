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
 * A EOF message for current context in protocol 4.1.
 */
final class Eof41Message implements EofMessage, WarningMessage {

    static final int SIZE = Byte.BYTES + (Short.BYTES << 1);

    private final int warnings;

    private final short serverStatuses;

    private Eof41Message(int warnings, short serverStatuses) {
        this.warnings = warnings;
        this.serverStatuses = serverStatuses;
    }

    @Override
    public int getWarnings() {
        return warnings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Eof41Message)) {
            return false;
        }

        Eof41Message that = (Eof41Message) o;

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
            return String.format("Eof41Message{warnings=%d, serverStatuses=%s}", warnings, serverStatuses);
        } else {
            return String.format("Eof41Message{serverStatuses=%s}", serverStatuses);
        }
    }

    static Eof41Message decode(ByteBuf buf) {
        buf.skipBytes(1); // skip generic header 0xFE of EOF messages

        int warnings = buf.readUnsignedShortLE();
        return new Eof41Message(warnings, buf.readShortLE());
    }
}
