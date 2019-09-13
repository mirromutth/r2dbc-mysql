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

package dev.miku.r2dbc.mysql.message.server;

import io.netty.buffer.ByteBuf;

/**
 * A server message after prepare query sent, includes prepare statement ID and other information.
 */
public final class PreparedOkMessage implements ServerMessage, WarningMessage {

    private static final int MIN_SIZE = Byte.BYTES + Integer.BYTES + Short.BYTES + Short.BYTES + Byte.BYTES;

    private static final int WARNING_SIZE = MIN_SIZE + Short.BYTES;

    private final int statementId;

    private final int totalColumns;

    private final int totalParameters;

    private final int warnings;

    private PreparedOkMessage(int statementId, int totalColumns, int totalParameters, int warnings) {
        this.statementId = statementId;
        this.totalColumns = totalColumns;
        this.totalParameters = totalParameters;
        this.warnings = warnings;
    }

    public int getStatementId() {
        return statementId;
    }

    public int getTotalColumns() {
        return totalColumns;
    }

    public int getTotalParameters() {
        return totalParameters;
    }

    @Override
    public int getWarnings() {
        return warnings;
    }

    static boolean isLooksLike(ByteBuf buf) {
        int readerIndex = buf.readerIndex();
        int readableBytes = buf.readableBytes();

        return (readableBytes == MIN_SIZE || readableBytes == WARNING_SIZE) &&
            buf.getByte(readerIndex) == 0 && buf.getByte(readerIndex + 9) == 0;
    }

    static PreparedOkMessage decode(ByteBuf buf) {
        buf.skipBytes(1); // constant 0x00
        int statementId = buf.readIntLE();
        int totalColumns = buf.readUnsignedShortLE();
        int totalParameters = buf.readUnsignedShortLE();
        buf.skipBytes(1); // constant filler, 0x00
        int warnings;

        if (buf.isReadable(2)) {
            warnings = buf.readUnsignedShortLE();
        } else {
            warnings = 0;
        }

        return new PreparedOkMessage(statementId, totalColumns, totalParameters, warnings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PreparedOkMessage)) {
            return false;
        }

        PreparedOkMessage that = (PreparedOkMessage) o;

        if (statementId != that.statementId) {
            return false;
        }
        if (totalColumns != that.totalColumns) {
            return false;
        }
        if (totalParameters != that.totalParameters) {
            return false;
        }
        return warnings == that.warnings;

    }

    @Override
    public int hashCode() {
        int result = statementId;
        result = 31 * result + totalColumns;
        result = 31 * result + totalParameters;
        result = 31 * result + warnings;
        return result;
    }

    @Override
    public String toString() {
        if (warnings != 0) {
            return String.format("PreparedOkMessage{statementId=%d, totalColumns=%d, totalParameters=%d, warnings=%d}", statementId, totalColumns, totalParameters, warnings);
        } else {
            return String.format("PreparedOkMessage{statementId=%d, totalColumns=%d, totalParameters=%d}", statementId, totalColumns, totalParameters);
        }
    }
}
