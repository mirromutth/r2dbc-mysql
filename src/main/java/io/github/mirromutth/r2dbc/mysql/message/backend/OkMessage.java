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

import io.github.mirromutth.r2dbc.mysql.constant.Capability;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.util.CodecUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * OK message.
 * <p>
 * Note: OK message are also used to indicate EOF and EOF message are deprecated as of MySQL 5.7.5.
 */
public final class OkMessage implements BackendMessage {

    private final long affectedRows;

    /**
     * Last insert-id, not business id on table.
     */
    private final long lastInsertId;

    private final short serverStatuses;

    private final short warnings;

    private final String information;

    private OkMessage(
        long affectedRows,
        long lastInsertId,
        short serverStatuses,
        short warnings,
        String information
    ) {
        this.affectedRows = affectedRows;
        this.lastInsertId = lastInsertId;
        this.serverStatuses = serverStatuses;
        this.warnings = warnings;
        this.information = requireNonNull(information, "information must not be null");
    }

    static OkMessage decode(ByteBuf buf, MySqlSession session) {
        buf.skipBytes(1); // OK message header, 0x00 (or 0xFE maybe?)

        long affectedRows = CodecUtils.readVarInt(buf);
        long lastInsertId = CodecUtils.readVarInt(buf);
        short serverStatuses = buf.readShortLE();
        short warnings = buf.readShortLE();

        int capabilities = session.getClientCapabilities();
        Charset charset = session.getCollation().getCharset();

        if ((capabilities & Capability.SESSION_TRACK.getFlag()) != 0) {
            String information = CodecUtils.readVarIntSizedString(buf, charset);
            // ignore session state information, it is not human readable and useless for client
            return new OkMessage(affectedRows, lastInsertId, serverStatuses, warnings, information);
        } else {
            return new OkMessage(affectedRows, lastInsertId, serverStatuses, warnings, buf.toString(charset));
        }
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getLastInsertId() {
        return lastInsertId;
    }

    public short getServerStatuses() {
        return serverStatuses;
    }

    public short getWarnings() {
        return warnings;
    }

    public String getInformation() {
        return information;
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

        if (affectedRows != okMessage.affectedRows) {
            return false;
        }
        if (lastInsertId != okMessage.lastInsertId) {
            return false;
        }
        if (serverStatuses != okMessage.serverStatuses) {
            return false;
        }
        if (warnings != okMessage.warnings) {
            return false;
        }
        return information.equals(okMessage.information);
    }

    @Override
    public int hashCode() {
        int result = (int) (affectedRows ^ (affectedRows >>> 32));
        result = 31 * result + (int) (lastInsertId ^ (lastInsertId >>> 32));
        result = 31 * result + (int) serverStatuses;
        result = 31 * result + (int) warnings;
        result = 31 * result + information.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "OkMessage{" +
            "affectedRows=" + affectedRows +
            ", lastInsertId=" + lastInsertId +
            ", serverStatuses=" + serverStatuses +
            ", warnings=" + warnings +
            ", information='" + information + '\'' +
            '}';
    }
}
