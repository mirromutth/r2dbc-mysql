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

import dev.miku.r2dbc.mysql.Capability;
import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.util.VarIntUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * OK message, it may be a complete signal of command, or a succeed signal for the Connection Phase of
 * connection lifecycle.
 * <p>
 * Note: OK message are also used to indicate EOF and EOF message are deprecated as of MySQL 5.7.5.
 */
public final class OkMessage implements WarningMessage, ServerStatusMessage, CompleteMessage {

    private static final int MIN_SIZE = 7;

    private final long affectedRows;

    /**
     * Last insert-id, not business id on table.
     */
    private final long lastInsertId;

    private final short serverStatuses;

    private final int warnings;

    private final String information;

    private OkMessage(long affectedRows, long lastInsertId, short serverStatuses, int warnings,
        String information) {
        this.affectedRows = affectedRows;
        this.lastInsertId = lastInsertId;
        this.serverStatuses = serverStatuses;
        this.warnings = warnings;
        this.information = requireNonNull(information, "information must not be null");
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getLastInsertId() {
        return lastInsertId;
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
        if (!(o instanceof OkMessage)) {
            return false;
        }

        OkMessage okMessage = (OkMessage) o;

        return affectedRows == okMessage.affectedRows &&
            lastInsertId == okMessage.lastInsertId &&
            serverStatuses == okMessage.serverStatuses &&
            warnings == okMessage.warnings &&
            information.equals(okMessage.information);
    }

    @Override
    public int hashCode() {
        int result = (int) (affectedRows ^ (affectedRows >>> 32));
        result = 31 * result + (int) (lastInsertId ^ (lastInsertId >>> 32));
        result = 31 * result + serverStatuses;
        result = 31 * result + warnings;
        return 31 * result + information.hashCode();
    }

    @Override
    public String toString() {
        if (warnings == 0) {
            return "OkMessage{affectedRows=" + affectedRows + ", lastInsertId=" + lastInsertId +
                ", serverStatuses=" + Integer.toHexString(serverStatuses) + ", information='" + information +
                "'}";
        }

        return "OkMessage{affectedRows=" + affectedRows + ", lastInsertId=" + lastInsertId +
            ", serverStatuses=" + Integer.toHexString(serverStatuses) + ", warnings=" + warnings +
            ", information='" + information + "'}";
    }

    static boolean isValidSize(int bytes) {
        return bytes >= MIN_SIZE;
    }

    static OkMessage decode(ByteBuf buf, ConnectionContext context) {
        buf.skipBytes(1); // OK message header, 0x00 or 0xFE

        Capability capability = context.getCapability();
        long affectedRows = VarIntUtils.readVarInt(buf);
        long lastInsertId = VarIntUtils.readVarInt(buf);
        short serverStatuses;
        int warnings;

        if (capability.isProtocol41()) {
            serverStatuses = buf.readShortLE();
            warnings = buf.readUnsignedShortLE();
        } else if (capability.isTransactionAllowed()) {
            serverStatuses = buf.readShortLE();
            warnings = 0;
        } else {
            warnings = serverStatuses = 0;
        }

        if (buf.isReadable()) {
            Charset charset = context.getClientCollation().getCharset();
            int sizeAfterVarInt = VarIntUtils.checkNextVarInt(buf);

            if (sizeAfterVarInt < 0) {
                return new OkMessage(affectedRows, lastInsertId, serverStatuses, warnings,
                    buf.toString(charset));
            }

            int readerIndex = buf.readerIndex();
            long size = VarIntUtils.readVarInt(buf);
            String information;

            if (size > sizeAfterVarInt) {
                information = buf.toString(readerIndex, buf.writerIndex() - readerIndex, charset);
            } else {
                information = buf.toString(buf.readerIndex(), (int) size, charset);
            }

            // Ignore session track, it is not human readable and useless for R2DBC client.
            return new OkMessage(affectedRows, lastInsertId, serverStatuses, warnings, information);
        }

        // Maybe have no human-readable message
        return new OkMessage(affectedRows, lastInsertId, serverStatuses, warnings, "");
    }
}
