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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import reactor.core.publisher.Flux;

/**
 * An implementations of {@link SimpleStatementSupport} based on MySQL text query.
 */
final class TextSimpleStatement extends SimpleStatementSupport {

    TextSimpleStatement(Client client, Codecs codecs, ConnectionContext context, String sql) {
        super(client, codecs, context, sql);
    }

    @Override
    public Flux<MySqlResult> execute() {
        return QueryFlow.execute(client, sql)
            .map(messages -> MySqlResult.toResult(false, codecs, context, generatedKeyName, messages));
    }
}
