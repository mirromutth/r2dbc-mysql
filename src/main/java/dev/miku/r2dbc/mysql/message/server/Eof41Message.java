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

import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import io.netty.buffer.ByteBuf;

/**
 * A EOF message for current context in protocol 4.1.
 */
final class Eof41Message implements EofMessage, WarningMessage, ServerStatusMessage {

    static final int SIZE = Byte.BYTES + (Short.BYTES << 1);

    private final int warnings;

    private final short serverStatuses;

    private Eof41Message(int warnings, short serverStatuses) {
        this.warnings = warnings;
        this.serverStatuses = serverStatuses;
    }

    @Override
    public short getServerStatuses() {
        return serverStatuses;
    }

    @Override
    public int getWarnings() {
        return warnings;
    }

    @Override
    public boolean isDone() {
        return (serverStatuses & ServerStatuses.MORE_RESULTS_EXISTS) == 0;
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

        return warnings == that.warnings && serverStatuses == that.serverStatuses;
    }

    @Override
    public int hashCode() {
        return 31 * warnings + serverStatuses;
    }

    @Override
    public String toString() {
        if (warnings == 0) {
            return "Eof41Message{serverStatuses=" + Integer.toHexString(serverStatuses) + '}';
        }

        return "Eof41Message{warnings=" + warnings + ", serverStatuses=" +
            Integer.toHexString(serverStatuses) + '}';
    }

    static Eof41Message decode(ByteBuf buf) {
        buf.skipBytes(1); // skip generic header 0xFE of EOF messages

        int warnings = buf.readUnsignedShortLE();
        return new Eof41Message(warnings, buf.readShortLE());
    }
}
