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

package io.github.mirromutth.r2dbc.mysql.core;

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.AuthType;
import io.github.mirromutth.r2dbc.mysql.security.AuthStateMachine;
import reactor.util.annotation.Nullable;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL sessions.
 */
public final class MySqlSession {

    private final int connectionId;

    private final ServerVersion serverVersion;

    private final int serverCapabilities;

    private final CharCollation collation;

    private final String database;

    private volatile int clientCapabilities;

    /**
     * It would be null after connection phase completed.
     */
    @Nullable
    private volatile AuthType authType;

    /**
     * It would be null after connection phase completed.
     */
    @Nullable
    private volatile AuthStateMachine authStateMachine;

    /**
     * It would be null after connection phase completed.
     */
    @Nullable
    private volatile String username;

    /**
     * It would be null after connection phase completed.
     */
    @Nullable
    private volatile String password;

    /**
     * It would be null after connection phase completed.
     */
    @Nullable
    private volatile byte[] salt;

    /**
     * It would be null after connection phase completed.
     */
    @Nullable
    private volatile byte[] authMoreData;

    public MySqlSession(
        int connectionId,
        ServerVersion serverVersion,
        int serverCapabilities,
        CharCollation collation,
        String database,
        int clientCapabilities,
        String username,
        @Nullable String password,
        byte[] salt,
        AuthType authType
    ) {
        this.connectionId = connectionId;
        this.serverVersion = requireNonNull(serverVersion, "serverVersion must not be null");
        this.serverCapabilities = serverCapabilities;
        this.collation = requireNonNull(collation, "collation must not be null");
        this.database = requireNonNull(database, "database must not be null");
        this.clientCapabilities = clientCapabilities;
        this.username = requireNonNull(username, "username must not be null");
        this.password = password;
        this.salt = requireNonNull(salt, "salt must not be null");
        this.authType = requireNonNull(authType, "authType must not be null");
        this.authStateMachine = AuthStateMachine.build(authType);
    }

    public int getConnectionId() {
        return connectionId;
    }

    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    public int getServerCapabilities() {
        return serverCapabilities;
    }

    public void setClientCapabilities(int clientCapabilities) {
        this.clientCapabilities = clientCapabilities;
    }

    public CharCollation getCollation() {
        return collation;
    }

    public String getDatabase() {
        return database;
    }

    public int getClientCapabilities() {
        return clientCapabilities;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public String getPassword() {
        return password;
    }

    @Nullable
    public byte[] getSalt() {
        return salt;
    }

    @Nullable
    public byte[] getAuthMoreData() {
        return authMoreData;
    }

    public void setAuthMoreData(byte[] authMoreData) {
        this.authMoreData = requireNonNull(authMoreData, "authMoreData must not be null");
    }

    @Nullable
    public AuthType getAuthType() {
        return authType;
    }

    /**
     * Generate current authentication and make changes to the authentication status.
     *
     * @return {@code null}
     */
    @Nullable
    public byte[] nextAuthentication() {
        AuthStateMachine authStateMachine = this.authStateMachine;

        if (authStateMachine == null) {
            return null;
        }

        return authStateMachine.nextAuthentication(this);
    }

    /**
     * All authentication data should be remove when connection phase completed or client closed in connection phase.
     */
    public void clearAuthentication() {
        this.username = null;
        this.password = null;
        this.salt = null;
        this.authType = null;
        this.authStateMachine = null;
        this.authMoreData = null;
    }
}
