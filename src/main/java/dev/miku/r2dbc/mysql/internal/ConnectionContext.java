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

package dev.miku.r2dbc.mysql.internal;

import dev.miku.r2dbc.mysql.ServerVersion;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.ServerStatuses;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;

/**
 * The MySQL connection context considers the behavior of server or client.
 * <p>
 * WARNING: It is internal data structure, do NOT use it outer than {@literal r2dbc-mysql},
 * try configure {@code ConnectionFactoryOptions} or {@code MySqlConnectionConfiguration}
 * to control connection context and client behavior.
 */
public final class ConnectionContext {

    private static final ServerVersion NONE_VERSION = ServerVersion.create(0, 0, 0);

    private volatile int connectionId = -1;

    private volatile ServerVersion serverVersion = NONE_VERSION;

    private final String database;

    private final ZeroDateOption zeroDateOption;

    /**
     * Client character collation.
     */
    private final CharCollation collation = CharCollation.clientCharCollation();

    /**
     * Assume that the auto commit is always turned on, it will be set after handshake V10 request message,
     * or OK message which means handshake V9 completed.
     */
    private volatile short serverStatuses = ServerStatuses.AUTO_COMMIT;

    private volatile int capabilities = 0;

    public ConnectionContext(String database, ZeroDateOption zeroDateOption) {
        this.database = AssertUtils.requireNonNull(database, "database must not be null");
        this.zeroDateOption = AssertUtils.requireNonNull(zeroDateOption, "zeroDateOption must not be null");
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    public void setServerVersion(ServerVersion serverVersion) {
        this.serverVersion = serverVersion;
    }

    public CharCollation getCollation() {
        return collation;
    }

    public String getDatabase() {
        return database;
    }

    public ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    public short getServerStatuses() {
        return serverStatuses;
    }

    public void setServerStatuses(short serverStatuses) {
        this.serverStatuses = serverStatuses;
    }

    public int getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(int capabilities) {
        this.capabilities = capabilities;
    }
}
