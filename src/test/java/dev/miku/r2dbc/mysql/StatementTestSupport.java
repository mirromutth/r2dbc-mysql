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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class considers unit tests for implementations of {@link MySqlStatement}.
 */
interface StatementTestSupport<T extends MySqlStatementSupport> {

    String PARAMETRIZED = "SELECT * FROM test WHERE id = ?id AND name = ?";

    String SIMPLE = "SELECT * FROM test WHERE id = 1 AND name = 'Mirrors'";

    T makeInstance(String parametrizedSql, String simpleSql);

    boolean supportsBinding();

    default int getFetchSize(T statement) throws IllegalAccessException {
        return -1;
    }

    @Test
    default void bind() {
        assertTrue(supportsBinding(), "Must skip test case #bind() for simple statements");

        T statement = makeInstance(PARAMETRIZED, SIMPLE);
        statement.bind(0, 1);
        statement.bind("id", 1);
        statement.bind(1, 1);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    default void badBind() {
        T statement = makeInstance(PARAMETRIZED, SIMPLE);

        if (supportsBinding()) {
            assertThrows(IllegalArgumentException.class, () -> statement.bind(0, null));
            assertThrows(IllegalArgumentException.class, () -> statement.bind("id", null));
            assertThrows(IllegalArgumentException.class, () -> statement.bind(null, 1));
            assertThrows(IllegalArgumentException.class, () -> statement.bind(null, null));
            assertThrows(IndexOutOfBoundsException.class, () -> statement.bind(-1, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> statement.bind(2, 1));
            assertThrows(IllegalArgumentException.class, () -> statement.bind(1, null));
            assertThrows(IllegalArgumentException.class, () -> statement.bind("", 1));
            assertThrows(IllegalArgumentException.class, () -> statement.bind("", null));
        } else {
            assertThrows(UnsupportedOperationException.class, () -> statement.bind(0, 1));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind("id", 1));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind(1, 1));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind(0, null));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind("id", null));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind(null, 1));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind(null, null));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind(-1, 1));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind(2, 1));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind(1, null));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind("", 1));
            assertThrows(UnsupportedOperationException.class, () -> statement.bind("", null));
        }
    }

    @Test
    default void bindNull() {
        assertTrue(supportsBinding(), "Must skip test case #bindNull() for simple statements");

        T statement = makeInstance(PARAMETRIZED, SIMPLE);
        statement.bindNull(0, Integer.class);
        statement.bindNull("id", Integer.class);
        statement.bindNull(1, Integer.class);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    default void badBindNull() {
        T statement = makeInstance(PARAMETRIZED, SIMPLE);

        if (supportsBinding()) {
            assertThrows(IllegalArgumentException.class, () -> statement.bindNull(0, null));
            assertThrows(IllegalArgumentException.class, () -> statement.bindNull("id", null));
            assertThrows(IllegalArgumentException.class, () -> statement.bindNull(null, Integer.class));
            assertThrows(IllegalArgumentException.class, () -> statement.bindNull(null, null));
            assertThrows(IndexOutOfBoundsException.class, () -> statement.bindNull(-1, Integer.class));
            assertThrows(IndexOutOfBoundsException.class, () -> statement.bindNull(2, Integer.class));
            assertThrows(IllegalArgumentException.class, () -> statement.bindNull(1, null));
            assertThrows(IllegalArgumentException.class, () -> statement.bindNull("", Integer.class));
            assertThrows(IllegalArgumentException.class, () -> statement.bindNull("", null));
        } else {
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull(0, Integer.class));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull("id", Integer.class));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull(1, Integer.class));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull(0, null));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull("id", null));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull(null, Integer.class));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull(null, null));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull(-1, Integer.class));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull(2, Integer.class));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull(1, null));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull("", Integer.class));
            assertThrows(UnsupportedOperationException.class, () -> statement.bindNull("", null));
        }
    }

    @Test
    default void add() {
        T statement = makeInstance(PARAMETRIZED, SIMPLE);

        statement.add();

        if (supportsBinding()) {
            statement.bind(0, 1);
            statement.bind(1, "");
            statement.add();
        }

        statement.add();
    }

    @Test
    default void badAdd() {
        assertTrue(supportsBinding(), "Must skip test case #badAdd() for simple statements");

        T statement = makeInstance(PARAMETRIZED, SIMPLE);
        statement.bind(0, 1);
        assertThrows(IllegalStateException.class, statement::add);
    }

    @Test
    default void returnGeneratedValues() {
        T statement = makeInstance(PARAMETRIZED, SIMPLE);

        statement.returnGeneratedValues();
        assertEquals(statement.generatedKeyName, "LAST_INSERT_ID");
        statement.returnGeneratedValues("generated");
        assertEquals(statement.generatedKeyName, "generated");
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    default void badReturnGeneratedValues() {
        T statement = makeInstance(PARAMETRIZED, SIMPLE);

        assertThrows(IllegalArgumentException.class, () -> statement.returnGeneratedValues((String) null));
        assertThrows(IllegalArgumentException.class, () -> statement.returnGeneratedValues((String[]) null));
        assertThrows(IllegalArgumentException.class, () -> statement.returnGeneratedValues(""));
        assertThrows(IllegalArgumentException.class, () -> statement.returnGeneratedValues("`generating`"));
        assertThrows(IllegalArgumentException.class, () -> statement.returnGeneratedValues("generating`"));
        assertThrows(IllegalArgumentException.class, () -> statement.returnGeneratedValues("generated", "names"));
    }

    @Test
    default void fetchSize() throws IllegalAccessException {
        T statement = makeInstance(PARAMETRIZED, SIMPLE);
        assertEquals(0, getFetchSize(statement), "Must skip test case #fetchSize() for text-based queries");

        for (int i = 1; i <= 10; ++i) {
            statement.fetchSize(i);
            assertEquals(i, getFetchSize(statement));
        }

        statement.fetchSize(Byte.MAX_VALUE);
        assertEquals(Byte.MAX_VALUE, getFetchSize(statement));
        statement.fetchSize(Short.MAX_VALUE);
        assertEquals(Short.MAX_VALUE, getFetchSize(statement));
        statement.fetchSize(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, getFetchSize(statement));
    }

    @Test
    default void badFetchSize() {
        T statement = makeInstance(PARAMETRIZED, SIMPLE);

        assertThrows(IllegalArgumentException.class, () -> statement.fetchSize(-1));
        assertThrows(IllegalArgumentException.class, () -> statement.fetchSize(-10));
        assertThrows(IllegalArgumentException.class, () -> statement.fetchSize(Integer.MIN_VALUE));
    }
}
