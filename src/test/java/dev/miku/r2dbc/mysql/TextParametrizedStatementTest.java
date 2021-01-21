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
import io.netty.buffer.UnpooledByteBufAllocator;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link TextParametrizedStatement}.
 */
class TextParametrizedStatementTest implements StatementTestSupport<TextParametrizedStatement> {

    private final Client client = mock(Client.class);

    private final ConnectionContext context = ConnectionContextTest.mock();

    private final Codecs codecs = Codecs.builder(UnpooledByteBufAllocator.DEFAULT).build();

    @Override
    public void fetchSize() {
        // No-op
    }

    @Override
    public TextParametrizedStatement makeInstance(String sql, String ignored) {
        return new TextParametrizedStatement(client, codecs, Query.parse(sql), context);
    }

    @Override
    public boolean supportsBinding() {
        return true;
    }
}
