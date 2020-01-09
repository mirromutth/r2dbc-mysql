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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ParametrizedStatementSupport} based on MySQL prepare query.
 */
final class PrepareParametrizedStatement extends ParametrizedStatementSupport {

    private final PrepareQuery query;

    private final boolean deprecateEof;

    private int fetchSize = 0;

    PrepareParametrizedStatement(Client client, Codecs codecs, ConnectionContext context, PrepareQuery query, boolean deprecateEof) {
        super(client, codecs, context, requireNonNull(query, "query must not be null").getParameters());
        this.query = query;
        this.deprecateEof = deprecateEof;
    }

    @Override
    public Flux<MySqlResult> execute(List<Binding> bindings) {
        String sql = query.getSql();
        return QueryFlow.prepare(client, sql)
            .doOnCancel(bindings::clear)
            .flatMapMany(it -> QueryFlow.execute(client, context, sql, it, deprecateEof, fetchSize, bindings)
                .map(messages -> new MySqlResult(true, codecs, context, generatedKeyName, messages))
                .onErrorResume(e -> it.close().then(Mono.error(e)))
                .concatWith(it.close().then(Mono.empty()))
                .doOnCancel(() -> it.close().subscribe()));
    }

    @Override
    public MySqlStatement fetchSize(int rows) {
        require(rows >= 0, "Fetch size must be greater or equal to zero");

        this.fetchSize = rows;
        return this;
    }

    @Override
    protected ParameterIndex getIndexes(String name) {
        return query.getIndexes(name);
    }
}
