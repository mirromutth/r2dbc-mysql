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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.ServerVersion;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;

import java.time.ZoneId;

/**
 * Codec variables context for encoding/decoding.
 */
public interface CodecContext {

    /**
     * Get the {@link ZoneId} of server-side.
     *
     * @return the {@link ZoneId}.
     */
    ZoneId getServerZoneId();

    /**
     * Get the option for zero date handling which is set by connection configuration.
     *
     * @return the {@link ZeroDateOption}.
     */
    ZeroDateOption getZeroDateOption();

    /**
     * Get the MySQL server version, which is available after database user logon.
     *
     * @return the {@link ServerVersion}.
     */
    ServerVersion getServerVersion();

    /**
     * Get the {@link CharCollation} that the client is using.
     *
     * @return the {@link CharCollation}.
     */
    CharCollation getClientCollation();
}
