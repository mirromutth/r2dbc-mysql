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

package dev.miku.r2dbc.mysql.message.client;

/**
 * A plain text SQL query message without any parameter, it could include multi-statements.
 */
public final class SimpleQueryMessage extends AbstractQueryMessage implements ExchangeableMessage {

    private static final byte QUERY_FLAG = 3;

    public SimpleQueryMessage(String sql) {
        super(QUERY_FLAG, sql);
    }

    @Override
    public String toString() {
        // SQL should NOT be printed as this may contain security information.
        // Of course, if user use trace level logs, SQL is still be printed by ByteBuf dump.
        return "SimpleQueryMessage{sql=REDACTED}";
    }
}
