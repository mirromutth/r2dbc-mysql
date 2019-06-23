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

import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ParsedQuery}.
 */
class ParsedQueryTest {

    @Test
    void hasParameter() {
        assertFalse(ParsedQuery.hasParameter("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')"));
        assertFalse(ParsedQuery.hasParameter("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` = '?name' AND `good` = \"dsa?\" /* ??? */ -- ??"));

        // Note: constant condition, useless, just for test.
        assertTrue(ParsedQuery.hasParameter("SELECT * FROM `user` WHERE (? = ?)"));

        // Note: wrong syntax, just for test.
        assertTrue(ParsedQuery.hasParameter("?"));
        assertTrue(ParsedQuery.hasParameter("?n"));
        assertTrue(ParsedQuery.hasParameter("?name"));
        assertTrue(ParsedQuery.hasParameter("? SELECT"));
        assertTrue(ParsedQuery.hasParameter("?n SELECT"));
        assertTrue(ParsedQuery.hasParameter("?name SELECT"));

        assertTrue(ParsedQuery.hasParameter("SELECT * FROM `user` WHERE `id` = ?"));
        assertTrue(ParsedQuery.hasParameter("SELECT * FROM `user` WHERE `id` = ?i"));
        assertTrue(ParsedQuery.hasParameter("SELECT * FROM `user` WHERE `id` = ?id"));
        assertTrue(ParsedQuery.hasParameter("INSERT INTO `user` VALUES (?, ?, ?, ?)"));
        assertTrue(ParsedQuery.hasParameter("INSERT INTO `user` VALUES (?, ?a, ?, ?b)"));
        assertTrue(ParsedQuery.hasParameter("INSERT INTO `user` VALUES (?a, ?b, ?c, ?d)"));
        assertTrue(ParsedQuery.hasParameter("UPDATE `user` SET `name` = ?"));
        assertTrue(ParsedQuery.hasParameter("UPDATE `user` SET `name` = ?n"));
        assertTrue(ParsedQuery.hasParameter("UPDATE `user` SET `name` = ?name"));
        assertTrue(ParsedQuery.hasParameter("UPDATE `user` SET `name` = ?name, `good` = ?"));
        assertTrue(ParsedQuery.hasParameter("UPDATE `user` SET `name` = ?, `good` = ?g"));
    }

    @Test
    void parse() {
        // Note: not be parametrized query, just for test.
        assertEmptyParseQuery("INSERT INTO `user` (`id`, `name`) VALUES (1, 'hello')");
        assertEmptyParseQuery("SELECT * FROM `user ? hello` WHERE `id` = '?' AND `name` = '?name' AND `good` = \"dsa?\" /* ??? */ -- ??");

        assertParseQuery("SELECT * FROM `user` WHERE (? = ?)", "SELECT * FROM `user` WHERE (? = ?)", Collections.emptyMap(), 2);
        assertParseQuery("?", "?", Collections.emptyMap(), 1);
        assertParseQuery("?n", "?", mapOf(link("n", 0)), 1);
        assertParseQuery("?name", "?", mapOf(link("name", 0)), 1);
        assertParseQuery("? SELECT", "? SELECT", Collections.emptyMap(), 1);
        assertParseQuery("?n SELECT", "? SELECT", mapOf(link("n", 0)), 1);
        assertParseQuery("?name SELECT", "? SELECT", mapOf(link("name", 0)), 1);

        assertParseQuery("SELECT * FROM `user` WHERE `id` = ?", "SELECT * FROM `user` WHERE `id` = ?", Collections.emptyMap(), 1);
        assertParseQuery("SELECT * FROM `user` WHERE `id` = ?i", "SELECT * FROM `user` WHERE `id` = ?", mapOf(link("i", 0)), 1);
        assertParseQuery("SELECT * FROM `user` WHERE `id` = ?id", "SELECT * FROM `user` WHERE `id` = ?", mapOf(link("id", 0)), 1);
        assertParseQuery("INSERT INTO `user` VALUES (?, ?, ?, ?)", "INSERT INTO `user` VALUES (?, ?, ?, ?)", Collections.emptyMap(), 4);
        assertParseQuery("INSERT INTO `user` VALUES (?, ?a, ?, ?b)", "INSERT INTO `user` VALUES (?, ?, ?, ?)", mapOf(link("a", 1), link("b", 3)), 4);
        assertParseQuery("INSERT INTO `user` VALUES (?a, ?b, ?c, ?d)", "INSERT INTO `user` VALUES (?, ?, ?, ?)", mapOf(link("a", 0), link("b", 1), link("c", 2), link("d", 3)), 4);

        assertParseQuery("UPDATE `user` SET `name` = ?", "UPDATE `user` SET `name` = ?", Collections.emptyMap(), 1);
        assertParseQuery("UPDATE `user` SET `name` = ?n", "UPDATE `user` SET `name` = ?", mapOf(link("n", 0)), 1);
        assertParseQuery("UPDATE `user` SET `name` = ?name", "UPDATE `user` SET `name` = ?", mapOf(link("name", 0)), 1);
        assertParseQuery("UPDATE `user` SET `name` = ?name, `good` = ?", "UPDATE `user` SET `name` = ?, `good` = ?", mapOf(link("name", 0)), 2);
        assertParseQuery("UPDATE `user` SET `name` = ?, `good` = ?g", "UPDATE `user` SET `name` = ?, `good` = ?", mapOf(link("g", 1)), 2);

        assertParseQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            Collections.emptyMap(),
            5
        );
        assertParseQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?) ON DUPLICATE KEY UPDATE `name` = ?name",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?",
            mapOf(link("name", 1, 3)),
            4
        );
        assertParseQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?age) ON DUPLICATE KEY UPDATE `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `age` = ?",
            mapOf(link("age", 2, 3)),
            4
        );
        assertParseQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            mapOf(link("name", 1, 3), link("age", 2, 4)),
            5
        );
        assertParseQuery(
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?id, ?name, ?age) ON DUPLICATE KEY UPDATE `name` = ?name, `age` = ?age",
            "INSERT INTO `user` (`id`, `name`, `age`) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `age` = ?",
            mapOf(link("id", 0), link("name", 1, 3), link("age", 2, 4)),
            5
        );
    }

    private static void assertParseQuery(String sql, String parsedSql, Map<String, int[]> nameKeyedIndexes, int parameters) {
        ParsedQuery query = ParsedQuery.parse(sql);
        assertEquals(query.getSql(), parsedSql);
        assertEquals(query.getParameters(), parameters);
        assertEquals(query.getParameterNames(), nameKeyedIndexes.keySet());
        for (Map.Entry<String, int[]> entry : nameKeyedIndexes.entrySet()) {
            assertArrayEquals(query.getIndexes(entry.getKey()), entry.getValue());
        }
    }

    private static void assertEmptyParseQuery(String sql) {
        assertParseQuery(sql, sql, Collections.emptyMap(), 0);
    }

    private static Tuple2<String, int[]> link(String name, int... indexes) {
        if (indexes.length == 0) {
            throw new IllegalArgumentException("must has least one index");
        }

        return Tuples.of(name, indexes);
    }

    @SafeVarargs
    private static Map<String, int[]> mapOf(Tuple2<String, int[]>... tuples) {
        // ceil(size / 0.75) = ceil((size * 4) / 3) = floor((size * 4 + 3 - 1) / 3)
        Map<String, int[]> result = new HashMap<>(((tuples.length << 2) + 2) / 3, 0.75f);

        for (Tuple2<String, int[]> tuple : tuples) {
            result.put(tuple.getT1(), tuple.getT2());
        }

        return result;
    }
}
