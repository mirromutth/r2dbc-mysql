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

package io.github.mirromutth.r2dbc.mysql;

import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

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
    MySqlStatement bind(Object identifier, Object value);

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement bind(int index, Object value);

    /**
     * {@inheritDoc}
     */
    default MySqlStatement bind(int index, boolean value) {
        return bind(index, (Boolean) value);
    }

    /**
     * {@inheritDoc}
     */
    default MySqlStatement bind(int index, byte value) {
        return bind(index, (Byte) value);
    }

    /**
     * {@inheritDoc}
     */
    default MySqlStatement bind(int index, char value) {
        return bind(index, (Character) value);
    }

    /**
     * {@inheritDoc}
     */
    default MySqlStatement bind(int index, double value) {
        return bind(index, (Double) value);
    }

    /**
     * {@inheritDoc}
     */
    default MySqlStatement bind(int index, float value) {
        return bind(index, (Float) value);
    }

    /**
     * {@inheritDoc}
     */
    default MySqlStatement bind(int index, int value) {
        return bind(index, (Integer) value);
    }

    /**
     * {@inheritDoc}
     */
    default MySqlStatement bind(int index, long value) {
        return bind(index, (Long) value);
    }

    /**
     * {@inheritDoc}
     */
    default MySqlStatement bind(int index, short value) {
        return bind(index, (Short) value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement bindNull(Object identifier, Class<?> type);

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement bindNull(int index, Class<?> type);

    /**
     * {@inheritDoc}
     */
    @Override
    MySqlStatement returnGeneratedValues(String... columns);

    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<MySqlResult> execute();
}
