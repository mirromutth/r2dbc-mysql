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
 * A message of prepare sql query for get prepared statement ID and information.
 */
public final class PrepareQueryMessage extends AbstractQueryMessage implements ExchangeableMessage {

    private static final byte PREPARE_FLAG = 0x16;

    public PrepareQueryMessage(CharSequence sql) {
        super(PREPARE_FLAG, sql);
    }

    @Override
    public String toString() {
        return "PrepareQueryMessage{sql=REDACTED}";
    }
}
