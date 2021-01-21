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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.codec.CodecContext;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import reactor.util.annotation.Nullable;

import java.time.ZoneId;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * The MySQL connection context considers the behavior of server or client.
 * <p>
 * WARNING: Do NOT change any data outside of this project, try configure {@code ConnectionFactoryOptions} or
 * {@code MySqlConnectionConfiguration} to control connection context and client behavior.
 */
public final class ConnectionContext implements CodecContext {

    private static final ServerVersion NONE_VERSION = ServerVersion.create(0, 0, 0);

    private volatile int connectionId = -1;

    private volatile ServerVersion serverVersion = NONE_VERSION;

    private final ZeroDateOption zeroDateOption;

    @Nullable
    private ZoneId serverZoneId;

    /**
     * Assume that the auto commit is always turned on, it will be set after handshake V10 request message, or
     * OK message which means handshake V9 completed.
     */
    private volatile short serverStatuses = ServerStatuses.AUTO_COMMIT;

    private volatile Capability capability = null;

    ConnectionContext(ZeroDateOption zeroDateOption, @Nullable ZoneId serverZoneId) {
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.serverZoneId = serverZoneId;
    }

    /**
     * Get the connection identifier that is specified by server.
     *
     * @return the connection identifier.
     */
    public int getConnectionId() {
        return connectionId;
    }

    /**
     * Initializes this context.
     *
     * @param connectionId the connection identifier that is specified by server.
     * @param version      the server version.
     * @param capability   the connection capabilities.
     */
    public void init(int connectionId, ServerVersion version, Capability capability) {
        this.connectionId = connectionId;
        this.serverVersion = version;
        this.capability = capability;
    }

    @Override
    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    @Override
    public CharCollation getClientCollation() {
        return CharCollation.clientCharCollation();
    }

    @Override
    public ZoneId getServerZoneId() {
        if (serverZoneId == null) {
            throw new IllegalStateException("Server timezone have not initialization");
        }
        return serverZoneId;
    }

    boolean shouldSetServerZoneId() {
        return serverZoneId == null;
    }

    void setServerZoneId(ZoneId serverZoneId) {
        if (this.serverZoneId != null) {
            throw new IllegalStateException("Server timezone have been initialized");
        }
        this.serverZoneId = serverZoneId;
    }

    @Override
    public ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    /**
     * Get the bitmap of server statuses.
     *
     * @return the bitmap.
     */
    public short getServerStatuses() {
        return serverStatuses;
    }

    /**
     * Updates server statuses.
     *
     * @param serverStatuses the bitmap of server statuses.
     */
    public void setServerStatuses(short serverStatuses) {
        this.serverStatuses = serverStatuses;
    }

    /**
     * Get the connection capability. Should use it after this context initialized.
     *
     * @return the connection capability.
     */
    public Capability getCapability() {
        return capability;
    }
}
