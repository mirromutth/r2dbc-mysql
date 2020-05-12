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

package dev.miku.r2dbc.mysql;

import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * A strongly typed implementation of {@link Statement} for the MySQL database.
 */
public interface MySqlStatement extends Statement {

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement add();

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement bind(int index, Object value);

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement bind(String name, Object value);

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement bindNull(int index, Class<?> type);

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement bindNull(String name, Class<?> type);

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement returnGeneratedValues(String... columns);

    /**
     * {@inheritDoc}
     */
    @Override
    Flux<MySqlResult> execute();

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement fetchSize(int rows);
}
