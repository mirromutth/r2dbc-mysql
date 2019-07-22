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

package io.github.mirromutth.r2dbc.mysql.message.client;

import io.github.mirromutth.r2dbc.mysql.constant.Capabilities;

import java.util.Map;

/**
 * TODO: write comment for this class or object
 */
public interface HandshakeResponse extends ExchangeableMessage {

    static HandshakeResponse from(
        int capabilities, int collationId, String username, byte[] authentication,
        String authType, String database, Map<String, String> attributes
    ) {
        if ((capabilities & Capabilities.PROTOCOL_41) == 0) {
            return new HandshakeResponse320(capabilities, username, authentication, database);
        } else {
            return new HandshakeResponse41(capabilities, collationId, username, authentication, authType, database, attributes);
        }
    }
}
