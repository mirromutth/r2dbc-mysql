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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link SimpleQuery}.
 */
class SimpleQueryTest extends QueryTestSupport {

    @Override
    @Test
    void parse() {
        assertSimpleQuery("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')");
        assertSimpleQuery("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello''world')");
        assertSimpleQuery("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` LIKE '?name' AND `good` = \"dsa?\" /* ??? */ -- ??");
    }

    @Override
    @Test
    void rejectGetIndexes() {
        assertThrows(IllegalArgumentException.class, () ->
            Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')", true).getIndexes("v"));
        assertThrows(IllegalArgumentException.class, () ->
            Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello''world')", true).getIndexes("v"));
        assertThrows(IllegalArgumentException.class, () ->
            Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` LIKE '?name' AND `good` = \"dsa?\" /* ??? */ -- ??", true)
                .getIndexes("v"));
    }

    @Override
    @Test
    void selfHashCode() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')", true);
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')", false);

        assertEquals(query1.hashCode(), query2.hashCode());

        query1 = Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` LIKE '?name' AND `good` = \"dsa?\" /* ??? */ -- ??", true);
        query2 = Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` LIKE '?name' AND `good` = \"dsa?\" /* ??? */ -- ??", false);

        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Override
    @Test
    void selfEquals() {
        Query query1 = Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')", true);
        Query query2 = Query.parse("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')", false);

        assertEquals(query1, query2);

        query1 = Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` LIKE '?name' AND `good` = \"dsa?\" /* ??? */ -- ??", true);
        query2 = Query.parse("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` LIKE '?name' AND `good` = \"dsa?\" /* ??? */ -- ??", false);

        assertEquals(query1, query2);
    }

    @Override
    void getIndexes() {
        // Noop
    }

    @Override
    void bind() {
        // Noop
    }

    @Override
    void indexesEquals() {
        // Noop
    }

    private static void assertSimpleQuery(String sql) {
        assertThat(Query.parse(sql, false)).isExactlyInstanceOf(SimpleQuery.class).isSameAs(SimpleQuery.INSTANCE);
    }
}
