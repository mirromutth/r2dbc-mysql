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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.constant.Capabilities;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;

/**
 * An abstraction of {@link ClientMessage} that considers SSL request for handshake.
 */
public interface SslRequest extends ClientMessage {

    int getCapabilities();

    static SslRequest from(int capabilities, int collationId) {
        require((capabilities & Capabilities.SSL) != 0, "capabilities must be SSL enabled");

        if ((capabilities & Capabilities.PROTOCOL_41) == 0) {
            return new SslRequest320(capabilities);
        } else {
            return new SslRequest41(capabilities, collationId);
        }
    }
}
